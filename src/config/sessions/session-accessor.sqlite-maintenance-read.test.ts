import path from "node:path";
import { expect, it, vi } from "vitest";
import { createDeferred, withinTest } from "../../../test/helpers/promise.js";
import { backfillSessionRowTranscriptFields } from "../../gateway/session-row-transcript-backfill.js";
import {
  closeOpenClawAgentDatabaseByPathAsync,
  runOpenClawAgentWriteTransaction,
} from "../../state/openclaw-agent-db.js";
import { withOpenClawTestState } from "../../test-utils/openclaw-test-state.js";
import { loadSessionEntry } from "./session-accessor.js";
import { writeSessionEntry } from "./session-accessor.sqlite-entry-store.js";
import type { SqliteSessionReclamationPlan } from "./session-accessor.sqlite-lifecycle-types.js";
import { kickSessionEntryMaintenanceAfterWrite } from "./session-accessor.sqlite-maintenance-kick.js";
import {
  createSessionMaintenanceKickStore,
  observeSessionMaintenancePlanningWorker,
} from "./session-accessor.sqlite-maintenance.test-support.js";
import * as reclamationRun from "./session-accessor.sqlite-reclamation-run.js";
import { runSqliteSessionReclamation } from "./session-accessor.sqlite-reclamation-run.js";
import { resolveSessionReclamationDatabaseOptions } from "./session-accessor.sqlite-reclamation.js";
import { replaceTranscriptEventsSync } from "./session-accessor.sqlite-transcript-write.test-support.js";
import { resolveMaintenanceConfigFromInput } from "./store-maintenance.js";

it.each(["durable", "warm durable", "direct active", "runtime"] as const)(
  "preserves %s capacity rows through prepared maintenance",
  async (protection) => {
    await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
      const options = { agentId: "main", env: state.env };
      const keys = ["agent:main:capacity-protected-0", "agent:main:capacity-protected-1"] as const;
      const write = (indexes: Array<0 | 1>) =>
        runOpenClawAgentWriteTransaction((database) => {
          for (const index of indexes) {
            const key = keys[index];
            writeSessionEntry(database, key, {
              sessionId: `capacity-protected-${index}`,
              updatedAt: Date.now(),
              modelSelectionLocked: protection === "durable" || protection === "warm durable",
            });
          }
        }, options);
      write(protection === "warm durable" ? [0] : [0, 1]);
      const plan = {
        kind: "maintenance-plan",
        databaseOptions: resolveSessionReclamationDatabaseOptions(options),
        materializedPlans: [],
        input: {
          activeSessionKeys:
            protection === "direct active" ? keys.map((key) => key.toUpperCase()) : [],
          archiveDirectory: state.sessionsDir(),
          storePath: path.join(state.sessionsDir(), "sessions.json"),
          maintenance: resolveMaintenanceConfigFromInput({
            mode: "enforce",
            maxEntries: 1,
            pruneAfter: "1d",
          }),
          preservation: {
            providerKeys: protection === "runtime" ? [...keys] : [],
            workIdentities: [],
            lifecycleIdentities: [],
          },
        },
      } satisfies SqliteSessionReclamationPlan;
      if (protection === "warm durable") {
        await expect(runSqliteSessionReclamation({ plan })).resolves.toMatchObject({
          kind: "maintenance-plan",
          ageFact: { entryCount: 1 },
        });
        write([1]);
      }
      if (protection === "durable" || protection === "warm durable") {
        await expect(runSqliteSessionReclamation({ plan })).resolves.toMatchObject({
          kind: "maintenance-plan",
          value: { archived: 0, durableNoCandidates: true },
        });
      } else {
        if (protection === "direct active") {
          const result = await runSqliteSessionReclamation({ plan });
          expect(result).toMatchObject({
            kind: "maintenance-plan",
            value: { archived: 0 },
          });
          if (result.kind === "maintenance-plan") {
            expect(result.value.durableNoCandidates).not.toBe(true);
          }
          plan.input.activeSessionKeys = [];
        }
        const result = await runSqliteSessionReclamation({ plan });
        expect(result.kind).toBe("maintenance-plan");
        if (result.kind === "maintenance-plan") {
          expect(result.value.archived).toBe(protection === "runtime" ? 0 : 1);
          expect(result.value.durableNoCandidates).not.toBe(true);
        }
      }
    });
  },
);

it("paces protected no-op retries after transcript backfill invalidates their age fact", async ({
  signal,
}) => {
  await withOpenClawTestState({ scenario: "minimal" }, async (state) => {
    const sessionKey = "agent:main:backfill-maintenance-owner";
    const { request, scope, storePath, updatedAt } = createSessionMaintenanceKickStore({
      storePath: state.statePath("backfill-maintenance.sqlite"),
      key: sessionKey,
      pruneAfterMs: 86_400_000,
      workerMaintenance: true,
      maintenanceScopes: [],
    });
    vi.useRealTimers();
    request.maintenanceConfig.maxEntries = 1;
    const lockedKey = "agent:main:backfill-locked-owner";
    runOpenClawAgentWriteTransaction((database) => {
      for (const key of [sessionKey, lockedKey]) {
        writeSessionEntry(database, key, { sessionId: key, updatedAt, modelSelectionLocked: true });
      }
    }, scope);
    expect(
      replaceTranscriptEventsSync(
        { agentId: "main", storePath, sessionKey, sessionId: sessionKey },
        [
          { type: "session", id: sessionKey, version: 3 },
          {
            type: "message",
            id: "backfill-message",
            parentId: null,
            message: { role: "user", content: "Synthetic backfill preview" },
          },
        ],
      ),
    ).toBe(true);
    let backfills = 0;
    observeSessionMaintenancePlanningWorker({
      async afterPrepare() {
        if (backfills >= 2) {
          return;
        }
        backfills++;
        await expect(
          backfillSessionRowTranscriptFields({
            agentId: "main",
            storePath,
            sessionKey,
            sessionId: sessionKey,
            sessionEntry: { sessionId: sessionKey, updatedAt },
          }),
        ).resolves.toEqual({ lastMessagePreview: "Synthetic backfill preview" });
      },
    });
    const retry = createDeferred<number>();
    const settled = createDeferred();
    let completedPlans = 0;
    const dispatch = vi.mocked(reclamationRun.runSqliteSessionReclamation);
    const run = dispatch.getMockImplementation()!;
    dispatch.mockImplementation(async (params) => {
      const result = await run(params);
      if (result.kind === "maintenance-plan") {
        completedPlans++;
        expect(result.value.archived).toBe(0);
        expect(result.value.durableNoCandidates).toBe(true);
        if (completedPlans <= 2) {
          expect(result.ageFact).toBeUndefined();
        } else {
          expect(result.ageFact?.entryCount).toBe(2);
          expect(result.nextAt).toBeGreaterThan(Date.now());
          settled.resolve();
        }
      }
      return result;
    });
    const setTimer = globalThis.setTimeout;
    const timers = vi.spyOn(globalThis, "setTimeout").mockImplementation((...args) => {
      const timer = setTimer(...args);
      const delay = args[1];
      // Worker deadlines and executor idle timers are longer than the maintenance quiet window.
      if (completedPlans === 2 && typeof delay === "number" && delay <= 1_000) {
        retry.resolve(delay);
      }
      return timer;
    });
    try {
      kickSessionEntryMaintenanceAfterWrite(request);
      expect(await withinTest(retry.promise, signal)).toBe(1_000);
      await withinTest(settled.promise, signal);
      expect(completedPlans).toBe(3);
      for (const key of [sessionKey, lockedKey]) {
        expect(loadSessionEntry({ storePath, sessionKey: key })?.archivedAt).toBeUndefined();
      }
    } finally {
      timers.mockRestore();
      await closeOpenClawAgentDatabaseByPathAsync(storePath);
      vi.restoreAllMocks();
    }
  });
});
