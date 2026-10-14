import { EventEmitter } from "node:events";
import { existsSync, readFileSync } from "node:fs";
import { PassThrough } from "node:stream";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { execGhReadAsync, execPlainGhAsync } from "../../scripts/lib/plain-gh.mjs";
import { execPrGhAsync } from "../../scripts/pr-lib/github.mjs";

const transport = vi.hoisted(() => ({ spawn: vi.fn() }));
vi.mock("node:child_process", async (importOriginal) => ({
  ...(await importOriginal<typeof import("node:child_process")>()),
  spawn: transport.spawn,
}));

type Request = {
  file: string;
  args: string[];
  options: { env: NodeJS.ProcessEnv; stdio: unknown[] };
  child: EventEmitter & {
    stdout: PassThrough;
    stderr: PassThrough;
    stdin: PassThrough;
    kill: ReturnType<typeof vi.fn>;
  };
};
let requests: Request[];
const env = { PATH: "/fixture", OPENCLAW_GH_BIN: process.execPath, GH_TOKEN: "fixture-token" };

beforeEach(() => {
  requests = [];
  transport.spawn.mockImplementation((file, args, options) => {
    const child = Object.assign(new EventEmitter(), {
      stdout: new PassThrough(),
      stderr: new PassThrough(),
      stdin: new PassThrough(),
      kill: vi.fn(),
    });
    requests.push({ file, args, options, child });
    return child;
  });
});
afterEach(() => vi.clearAllMocks());

function requestAt(index: number): Request {
  const request = requests[index];
  if (!request) {
    throw new Error(`Expected spawned request ${index}`);
  }
  return request;
}

function complete(request: Request, code = 0, stdout = "ok", stderr = "") {
  request.child.stdout.end(stdout);
  request.child.stderr.end(stderr);
  request.child.emit("close", code, null);
}

it("caps mixed writer and reader subprocesses at four and drains queued work", async () => {
  const tasks = Array.from({ length: 10 }, (_, index) =>
    (index % 2 ? execGhReadAsync : execPlainGhAsync)([String(index)], { env }),
  );
  expect(requests).toHaveLength(4);
  expect(requests.map((request) => request.file)).toEqual([
    process.execPath,
    "gh",
    process.execPath,
    "gh",
  ]);
  expect(requestAt(0).options.env.OPENCLAW_GH_BIN).toBe(process.execPath);
  expect(requestAt(1).options.env.OPENCLAW_GH_BIN).toBeUndefined();
  for (const request of requests.slice(0, 4)) {
    complete(request);
  }
  await Promise.all(tasks.slice(0, 4));
  expect(requests).toHaveLength(8);
  for (const request of requests.slice(4, 8)) {
    complete(request);
  }
  await Promise.all(tasks.slice(4, 8));
  expect(requests).toHaveLength(10);
  for (const request of requests.slice(8)) {
    complete(request);
  }
  await expect(Promise.all(tasks)).resolves.toEqual(Array(10).fill("ok"));
});

it("keeps the writer notifier and quota refusal semantics without retrying", async () => {
  const task = execPrGhAsync(
    ["api", "repos/fixture/repo", "--include"],
    {
      env: { ...env, OPENCLAW_PR_LOCK_NOTIFY_FD: "3" },
    },
    "plain",
  );
  expect(requestAt(0).options.stdio).toEqual(["ignore", "pipe", "pipe", 3]);
  complete(
    requestAt(0),
    1,
    'HTTP/2.0 403 Forbidden\r\nx-ratelimit-resource: core\r\nx-ratelimit-remaining: 0\r\nx-ratelimit-reset: 2000000000\r\n\r\n{"message":"API rate limit exceeded"}',
  );
  await expect(task).rejects.toMatchObject({
    code: "OPENCLAW_GH_ACCESS",
    status: 75,
    coreQuotaExhausted: true,
  });
  expect(requests).toHaveLength(1);
});

it("keeps input bytes private until the asynchronous child has closed", async () => {
  const task = execPrGhAsync(
    ["api", "graphql", "--input", "-"],
    {
      env,
      input: "fixture payload",
      stdio: ["pipe", "pipe", "pipe"],
    },
    "plain",
  );
  const request = requestAt(0);
  const path = request.args[request.args.indexOf("--input") + 1];
  if (!path) {
    throw new Error("Expected staged input path");
  }
  expect(readFileSync(path, "utf8")).toBe("fixture payload");
  expect(request.options.stdio[0]).toBe("ignore");
  complete(request);
  await expect(task).resolves.toBe("ok");
  expect(existsSync(path)).toBe(false);
});

it("waits for a failed child to close before releasing its concurrency slot", async () => {
  const tasks = Array.from({ length: 5 }, () => execGhReadAsync(["api"], { env, maxBuffer: 3 }));
  const first = requestAt(0);
  const failure = expect(tasks[0]).rejects.toMatchObject({
    code: "ERR_CHILD_PROCESS_STDIO_MAXBUFFER",
  });
  first.child.stdout.write("too large");
  expect(first.child.kill).toHaveBeenCalledWith("SIGTERM");
  expect(requests).toHaveLength(4);
  first.child.emit("close", null, "SIGTERM");
  await failure;
  expect(requests).toHaveLength(5);
  for (const request of requests.slice(1)) {
    complete(request);
  }
  await expect(Promise.all(tasks.slice(1))).resolves.toEqual(Array(4).fill("ok"));
});
