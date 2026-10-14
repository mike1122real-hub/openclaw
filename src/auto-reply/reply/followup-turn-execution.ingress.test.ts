import path from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import * as sandboxRuntime from "../../agents/sandbox/runtime-status.js";
import { bindIngressLifecycleToReplyOptions } from "../../channels/message/ingress-drain-lifecycle.js";
import { createChannelIngressDrain } from "../../channels/message/ingress-drain.js";
import {
  createTestIngressQueue,
  observeChannelIngressQueueWrite,
} from "../../channels/message/ingress-drain.test-helpers.js";
import { setRuntimeConfigSnapshot } from "../../config/runtime-snapshot.js";
import {
  loadSessionEntry,
  replaceSessionEntry,
} from "../../config/sessions/session-accessor.sqlite-entry.js";
import { loadSessionTranscriptEvents } from "../../config/sessions/session-transcript-events.js";
import type { SessionEntry } from "../../config/sessions/types.js";
import { createUserTurnTranscriptRecorder } from "../../sessions/user-turn-transcript.js";
import { withOpenClawTestState } from "../../test-utils/openclaw-test-state.js";
import type { ReplyPayload } from "../types.js";
import type { AdmittedFollowupTurn } from "./followup-turn-admission.js";
import {
  createFollowupTurnTestTurn,
  createFollowupTurnTestTypingController,
  executeFollowupTurnForTest,
  getFollowupTurnTestState,
  resetFollowupTurnTestState,
} from "./followup-turn-execution.test-support.js";
import { enqueueFollowupRun, isFollowupRunAborted, scheduleFollowupDrain } from "./queue.js";
import { clearFollowupQueueForTest, createQueueTestRun } from "./queue.test-helpers.js";
import { createReplyOperation } from "./reply-run-registry.js";
import { retainReplyOperationUntilComplete } from "./reply-run-registry.state.js";
import { createReplyRecoveryActorFixture } from "./restart-recovery-claim.test-support.js";

beforeEach(() => {
  resetFollowupTurnTestState();
  vi.useFakeTimers();
});
afterEach(() => {
  vi.clearAllTimers();
  vi.useRealTimers();
});

describe("queued ingress acceptance", () => {
  it("lets ingress bound delivery-group preparation failures before queued execution", async () => {
    await withOpenClawTestState({ label: "queued-ingress-group-retry" }, async (state) => {
      const preparation = vi
        .spyOn(sandboxRuntime, "withSandboxRuntimeStatusesInWorker")
        .mockRejectedValue(new Error("ENOSPC: sandbox status read failed"));
      const queue = createTestIngressQueue(state.stateDir);
      const failed = observeChannelIngressQueueWrite(queue, "fail", "second-input");
      const key = "agent:main:ingress-group-retry";
      const ingressSignals: AbortSignal[] = [];
      const runFollowup = vi.fn(async () => {});
      const drain = createChannelIngressDrain({
        queue,
        adoptionStallTimeoutMs: 90,
        retryPolicy: { maxAttempts: 1, deadLetterMinAgeMs: 0 },
        dispatchClaimedEvent: (event, lifecycle) => {
          ingressSignals.push(lifecycle.abortSignal);
          const run = createQueueTestRun({
            prompt: event.payload.text,
            messageId: event.id,
            originatingChannel: "discord",
            originatingTo: "channel:synthetic-group",
          });
          run.run.agentId = "main";
          run.run.sessionKey = key;
          run.run.gatewayUiCommandTarget = { connId: "synthetic-browser", profileId: "viewer" };
          run.turnAdoptionLifecycle =
            bindIngressLifecycleToReplyOptions(lifecycle).turnAdoptionLifecycle;
          enqueueFollowupRun(
            key,
            run,
            { mode: "collect", debounceMs: 10, cap: 2, dropPolicy: "summarize" },
            "message-id",
            runFollowup,
            false,
          );
          return { kind: "deferred" };
        },
      });
      try {
        await queue.enqueue("first-input", { text: "First request" }, { laneKey: "first" });
        await queue.enqueue("second-input", { text: "Second request" }, { laneKey: "second" });
        expect(await drain.drainOnce()).toEqual({ started: 2 });
        await drain.waitForIdle();
        scheduleFollowupDrain(key, runFollowup);
        await vi.advanceTimersByTimeAsync(50);
        expect(preparation).toHaveBeenCalled();
        expect(preparation.mock.calls[0]?.[0]).toHaveLength(2);
        expect(runFollowup).not.toHaveBeenCalled();
        expect(ingressSignals.map((signal) => signal.aborted)).toEqual([false, false]);
        await vi.advanceTimersByTimeAsync(60);
        expect(ingressSignals.map((signal) => signal.aborted)).toEqual([true, true]);
        await expect(failed).resolves.toBe(true);
        expect(await queue.listFailed?.()).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ id: "first-input", reason: "retry-limit-exceeded" }),
            expect.objectContaining({ id: "second-input", reason: "retry-limit-exceeded" }),
          ]),
        );
      } finally {
        drain.dispose();
        clearFollowupQueueForTest(key);
        preparation.mockRestore();
      }
    });
  });

  it.each(["followup", "collect"] as const)(
    "lets ingress bound repeated preparation failures in %s mode",
    async (mode) => {
      await withOpenClawTestState({ label: "queued-ingress-retry" }, async (state) => {
        const queue = createTestIngressQueue(state.stateDir);
        const failed = observeChannelIngressQueueWrite(queue, "fail");
        const key = `agent:main:ingress-retry-${mode}`;
        let ingressSignal: AbortSignal | undefined;
        const runFollowup = vi.fn(async (run: Parameters<typeof isFollowupRunAborted>[0]) => {
          if (!isFollowupRunAborted(run)) {
            throw new Error("ENOSPC: queued preparation failed");
          }
        });
        const drain = createChannelIngressDrain({
          queue,
          adoptionStallTimeoutMs: 90,
          retryPolicy: { maxAttempts: 1, deadLetterMinAgeMs: 0 },
          dispatchClaimedEvent: (_event, lifecycle) => {
            ingressSignal = lifecycle.abortSignal;
            const run = createQueueTestRun({ prompt: "Please answer", messageId: "failed-input" });
            run.turnAdoptionLifecycle =
              bindIngressLifecycleToReplyOptions(lifecycle).turnAdoptionLifecycle;
            enqueueFollowupRun(
              key,
              run,
              { mode, debounceMs: 10, cap: 2, dropPolicy: "summarize" },
              "message-id",
              runFollowup,
              false,
            );
            scheduleFollowupDrain(key, runFollowup);
            return { kind: "deferred" };
          },
        });
        try {
          await queue.enqueue("failed-input", { text: "Please answer" });
          await drain.drainOnce();
          await drain.waitForIdle();
          await vi.advanceTimersByTimeAsync(110);
          expect(runFollowup).toHaveBeenCalled();
          expect(ingressSignal?.aborted).toBe(true);
          await expect(failed).resolves.toBe(true);
          expect(await queue.listFailed?.()).toMatchObject([
            { id: "failed-input", reason: "retry-limit-exceeded" },
          ]);
        } finally {
          drain.dispose();
          clearFollowupQueueForTest(key);
        }
      });
    },
  );

  it.each(
    ["followup-collect", "followup-overflow"].flatMap((sourceKind) =>
      ["setup-failure", "adoption-timeout", "user-abort"].map((failure) => ({
        sourceKind,
        failure,
      })),
    ),
  )(
    "persists $sourceKind custody before ingress completion and handles $failure without replay",
    async ({ sourceKind, failure }) => {
      await withOpenClawTestState({ label: "queued-ingress-acceptance" }, async (state) => {
        setRuntimeConfigSnapshot({});
        const target = {
          agentId: "main",
          sessionKey: "agent:main:queued-ingress",
          sessionId: "queued-session",
          storePath: path.join(state.agentDir("main"), "openclaw-agent.sqlite"),
        };
        let entry: SessionEntry = { sessionId: target.sessionId, updatedAt: 1 };
        await replaceSessionEntry(target, entry);
        const upstreamAbort = new AbortController();
        const operation = createReplyOperation({
          ...target,
          resetTriggered: false,
          upstreamAbortSignal: upstreamAbort.signal,
        });
        retainReplyOperationUntilComplete(operation);
        await using actor = createReplyRecoveryActorFixture({
          ...target,
          operation,
          getSessionId: () => operation.sessionId,
        });
        await actor.bind();
        const sourceId = `${sourceKind}:queued-session:synthetic-input`;
        const recorder = createUserTurnTranscriptRecorder({
          input: { text: "queued user message", idempotencyKey: sourceId },
          target: { ...target, sessionEntry: entry },
        });
        const turn: AdmittedFollowupTurn = createFollowupTurnTestTurn({
          operation,
          session: {
            kind: "session",
            key: target.sessionKey,
            storePath: target.storePath,
            current: () => entry,
            publish: (next) => {
              if (next) {
                entry = next;
              }
            },
            adopt: (next) => {
              entry = next;
            },
          },
        });
        turn.queued.run = {
          ...turn.queued.run,
          agentId: target.agentId,
          sessionKey: target.sessionKey,
          sessionId: target.sessionId,
          agentDir: state.agentDir("main"),
          workspaceDir: state.workspaceDir,
        };
        turn.queued.sourceTurnId = "channel-user:v1:original-inbound";
        turn.queued.userTurnTranscriptRecorder = recorder;
        const execute = getFollowupTurnTestState().execute;
        execute.mockRejectedValue(
          Object.assign(new Error("no space left on device"), { code: "ENOSPC" }),
        );
        const queue = createTestIngressQueue(state.stateDir);
        const timedOut = failure !== "setup-failure";
        const ingressFailed = timedOut
          ? observeChannelIngressQueueWrite(queue, "fail", "source-message")
          : undefined;
        const replies: ReplyPayload[] = [];
        const adopted = vi.fn(async () => {
          expect(recorder.hasPersisted()).toBe(true);
          expect(loadSessionEntry(target)).toMatchObject({
            restartRecoveryDeliveryRunId: "message-1",
            restartRecoveryDeliverySourceRunId: sourceId,
          });
          expect(await loadSessionTranscriptEvents(target)).toEqual(
            expect.arrayContaining([
              expect.objectContaining({
                type: "message",
                message: expect.objectContaining({
                  role: "user",
                  content: "queued user message",
                  idempotencyKey: sourceId,
                }),
              }),
            ]),
          );
          expect(await queue.listClaims()).toHaveLength(1);
        });
        const drain = createChannelIngressDrain({
          queue,
          adoptionStallTimeoutMs: 90,
          retryPolicy: { maxAttempts: 1, deadLetterMinAgeMs: 0 },
          dispatchClaimedEvent: async (_event, lifecycle) => {
            lifecycle.abortSignal.addEventListener(
              "abort",
              () => upstreamAbort.abort(lifecycle.abortSignal.reason),
              { once: true },
            );
            turn.queued.abortSignal = lifecycle.abortSignal;
            turn.queued.turnAdoptionLifecycle = bindIngressLifecycleToReplyOptions({
              ...lifecycle,
              onAdopted: async () => {
                await adopted();
                if (timedOut) {
                  if (failure === "user-abort") {
                    expect(operation.abortByUser()).toBe(true);
                  }
                  await vi.advanceTimersByTimeAsync(100);
                  expect(operation.abortSignal.aborted).toBe(true);
                  expect(
                    loadSessionEntry(target)?.restartRecoveryTerminalRunIds ?? [],
                  ).not.toContain(sourceId);
                }
                await lifecycle.onAdopted();
              },
            }).turnAdoptionLifecycle;
            const result = await executeFollowupTurnForTest({
              turn,
              defaults: {
                typing: createFollowupTurnTestTypingController(),
                typingMode: "never",
                defaultModel: "test-model",
              },
              onToolResult: async () => {},
              onCompactionNoticePayload: async () => {},
            });
            if (result.execution.outcome.kind === "rejected") {
              replies.push(result.execution.outcome.payload);
            }
          },
        });
        try {
          await queue.enqueue("source-message", { text: "queued user message" });
          expect(await drain.drainOnce()).toEqual({ started: 1 });
          await drain.waitForIdle();
          expect(adopted).toHaveBeenCalledOnce();
          expect(execute).toHaveBeenCalledTimes(timedOut ? 0 : 1);
          expect(replies).toEqual(
            failure === "user-abort"
              ? []
              : [expect.objectContaining({ text: expect.stringContaining("⚠️") })],
          );
          if (ingressFailed) {
            await expect(ingressFailed).resolves.toBe(true);
          }
          expect(
            await queue.enqueue("source-message", { text: "queued user message" }),
          ).toMatchObject({ kind: timedOut ? "failed" : "completed" });
          expect(await drain.drainOnce()).toEqual({ started: 0 });
          await drain.waitForIdle();
          expect(execute).toHaveBeenCalledTimes(timedOut ? 0 : 1);
          expect(replies).toHaveLength(failure === "user-abort" ? 0 : 1);
        } finally {
          drain.dispose();
          await turn.restartRecovery?.clear();
        }
      });
    },
  );
});
