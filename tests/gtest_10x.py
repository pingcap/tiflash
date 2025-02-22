# Copyright 2025 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A 10x faster gtest runner.

import io
import dataclasses  # Python 3.7
import asyncio  # Python 3.4
import asyncio.subprocess
import asyncio.exceptions
import threading  # for IO related
import optparse
import sys
import os
import heapq
import shutil
import enum
import time
import re
import traceback
import json

# fmt: off
class C:  # Colors
    RESET     = "\033[0m"
    OK        = "\033[92m\033[1m"
    FAIL      = "\033[91m\033[1m"
    WARN      = "\033[93m\033[1m"
    DIM       = "\033[2m"
    TIME_OK   = "\033[90m"
    TIME_WARN = "\033[93m\033[1m"
class Out:
    OK         =     f"{C.OK}OK:      {C.RESET}"
    FAILED     =   f"{C.FAIL}FAILED:  {C.RESET}"
    FAILED_DIM =    f"{C.DIM}FAILED:  "
    SKIPPED    =   f"{C.WARN}SKIPPED: {C.RESET}"
# fmt: on


@dataclasses.dataclass
class Options:
    v: optparse.Values = None
    binaries: list[str] = dataclasses.field(default_factory=list)
    additional_args: list[str] = dataclasses.field(default_factory=list)
    skip_tests: set[str] = dataclasses.field(default_factory=set)
    suppress_tests: set[str] = dataclasses.field(default_factory=set)


@dataclasses.dataclass(order=True)
class TestCase:
    binary: str
    test_suite: str
    full_test_name: str
    last_duration: int  # From history


@dataclasses.dataclass(order=True)
class TestGroup:
    binary: str
    tests: set[str]


@dataclasses.dataclass
class TestStatus:
    @enum.unique
    class Result(enum.Enum):
        NOT_RUN = 0
        PASS = 1
        FAIL = 2
        FAIL_TIMEOUT = 3

        # Special result only used in summaries
        PASS_FLAKY = 4
        FAIL_SUPPRESSED = 5

    result: Result = Result.NOT_RUN
    start_at: float = 0
    end_at: float = 0
    n_runs: int = 0


class TestStatusRepo(dict[str, dict[str, TestStatus]]):
    pass


class History(dict[str, dict[str, int]]):
    @staticmethod
    def from_file(path: str) -> "History":
        try:
            with open(path, "r", encoding="utf-8") as f:
                return History(json.load(f))
        except FileNotFoundError:
            print(
                f"{C.WARN}WARN: {C.RESET}There is no run history file. Tests may be running slower than optimal."
            )
            return History()
        except json.JSONDecodeError:
            print(
                f"{C.WARN}WARN: {C.RESET}Failed to read run history file. Tests may be running slower than optimal."
            )
            return History()

    def persist(self, path: str):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self, f, indent=2)

    def get_ms(self, binary: str, test_name: str) -> int:
        return self.get(binary, {}).get(test_name, 1)


class Scheduler:
    """
    Run at most N tasks concurrently according to a priority queue.
    """

    class TaskWrapper:
        def __init__(self, priority: int, coro):
            self.priority = priority
            self.coro = coro

        def __lt__(self, other) -> bool:
            return self.priority < other.priority

    def __init__(self, max_concurrent: int):
        self.max_concurrent = max_concurrent
        self.priority_queue = []
        self.running_tasks = 0  # For concurrency control
        self.unfinished_tasks = 0  # For join
        self.finished_ev = asyncio.Event()
        self.finished_ev.set()
        self.is_shutdown = False

    def create_task(self, priority: int, coro):
        if self.is_shutdown:
            raise RuntimeError("Scheduler is already shutdown")
        self.unfinished_tasks += 1
        self.finished_ev.clear()
        task = self.TaskWrapper(priority, coro)
        heapq.heappush(self.priority_queue, task)
        self._try_schedule()

    def _try_schedule(self):
        while self.running_tasks < self.max_concurrent:
            if not self.priority_queue:
                break
            task = heapq.heappop(self.priority_queue)
            self._start_task(task)

    def _start_task(self, task: TaskWrapper):
        self.running_tasks += 1
        asyncio.create_task(self._task_fn(task))

    async def _task_fn(self, task: TaskWrapper):
        try:
            await task.coro
        except asyncio.CancelledError:
            self.is_shutdown = True
        finally:
            self.running_tasks -= 1
            self.unfinished_tasks -= 1
            if self.unfinished_tasks == 0:
                self.finished_ev.set()
            if not self.is_shutdown:
                self._try_schedule()

    async def _join(self):
        if self.unfinished_tasks > 0:
            await self.finished_ev.wait()

    async def wait_all(self) -> None:
        try:
            await self._join()
        except asyncio.CancelledError:
            for task in self.priority_queue:
                task.coro.close()
            self.priority_queue.clear()
            raise


class GlobalContext:
    def __init__(
        self,
        scheduler: Scheduler,
        status_repo: TestStatusRepo,
        history: History,
        sync_stdout_writer: io.BufferedWriter,
        sync_stderr_writer: io.BufferedWriter,
        options: Options,
    ):
        self.scheduler = scheduler
        self.status_repo = status_repo
        self.history = history
        self.sync_stdout_writer = sync_stdout_writer
        self.sync_stdout_writer_lock = threading.Lock()
        self.sync_stderr_writer = sync_stderr_writer
        self.sync_stderr_writer_lock = threading.Lock()
        self.options = options
        self.spawned_procs = 0
        self.finished_tests = 0
        self.all_tests = 0


class ProcWatcher:
    """
    Collect result from the execution of a single test binary.
    """

    # fmt: off
    r_run         = re.compile(r"^\[ RUN      \] (\S+)$")
    r_failed      = re.compile(r"^\[  FAILED  \] (\S+) \(\d+ ms\)$")
    r_failed_2    = re.compile(r"^\[  FAILED  \] (\S+), where (.+) \(\d+ ms\)$")
    r_ok          = re.compile(r"^\[       OK \] (\S+) \(\d+ ms\)$")
    # fmt: on

    max_lines_stdout = 2000
    max_bytes_stderr = 3 * 1024 * 1024  # 3MiB

    def __init__(self, ctx: GlobalContext, tests: TestGroup):
        self.proc = None
        self.ctx = ctx
        self.remaining_tests = tests
        self.log_writer_tasks = []

        # All status are updated into the global status_repo
        if self.remaining_tests.binary not in ctx.status_repo:
            ctx.status_repo[self.remaining_tests.binary] = {}
        self.status = ctx.status_repo[
            self.remaining_tests.binary
        ]  # Note: this is globally shared
        for test_name in self.remaining_tests.tests:
            n_runs = 0
            if test_name in self.status:
                n_runs = self.status[test_name].n_runs
            self.status[test_name] = TestStatus()
            if n_runs > 0:
                self.status[test_name].n_runs = n_runs

        self.current_logs_stdout: list[str] = []  # By lines
        self.current_logs_stderr: list[bytes] = []  # By blocks
        self.current_logs_stderr_size = 0
        self.current_test_name = ""
        self.has_timed_out = False
        self.test_timeout_task = None

    async def handle_proc(self, proc: asyncio.subprocess.Process):
        assert self.proc is None
        self.proc = proc
        is_user_interrupt = False
        tasks = [
            asyncio.create_task(self._handle_stdout(proc.stdout)),
            asyncio.create_task(self._handle_stderr(proc.stderr)),
        ]
        try:
            await asyncio.gather(*tasks)
            tasks.clear()
            # We only wait file write finish after the test execution.
            # If we wait write inside test execution, we will not be able to
            # handle real-time stdout and stderr streaming in time and lose
            # their order.
            await asyncio.gather(*self.log_writer_tasks)
            self.log_writer_tasks.clear()
        except asyncio.CancelledError:
            is_user_interrupt = True
            raise
        except Exception:
            # If any exception happens we cancel the current process
            # because this is not expected
            print(
                f"{C.FAIL}Meet Python exception when running test, a test will be aborted:{C.RESET}"
            )
            traceback.print_exception(sys.exception(), chain=True, colorize=True)
        finally:
            if self.test_timeout_task:
                self.test_timeout_task.cancel()
            for task in tasks:
                if not task.done():
                    task.cancel()
            for task in self.log_writer_tasks:
                if not task.done():
                    task.cancel()
            try:
                proc.kill()
            except ProcessLookupError:
                pass
            await proc.wait()
            if not is_user_interrupt:
                self._on_proc_exit()

    async def _handle_stdout(self, stream: asyncio.StreamReader):
        while not stream.at_eof():
            data = await stream.readline()
            line = data.decode("utf-8", errors="replace").rstrip()
            if len(self.current_logs_stdout) < self.max_lines_stdout:
                self.current_logs_stdout.append(line)
            try:
                if r := self.r_run.match(line):
                    self._on_test_run(r.group(1))
                elif r := self.r_ok.match(line):
                    self._on_test_pass(r.group(1))
                elif r := self.r_failed.match(line):
                    self._on_test_fail(r.group(1))
                elif r := self.r_failed_2.match(line):
                    self._on_test_fail(r.group(1))
            except KeyError as exc:
                raise RuntimeError(
                    f"Unable to find test from stdout line: {line}"
                ) from exc

    async def _handle_stderr(self, stream: asyncio.StreamReader):
        # There are a lot of stderr, so we only read and buffer them without
        # doing anything else.
        block_limit = 2**16  # 64KiB
        while not stream.at_eof():
            data = await stream.read(block_limit)
            self.current_logs_stderr.append(data)
            self.current_logs_stderr_size += len(data)
            if self.current_logs_stderr_size > self.max_bytes_stderr:
                # Discard remaining logs. We still need to keep reading
                # to avoid blocking the stderr pipe.
                while not stream.at_eof():
                    _ = await stream.read(block_limit)
                return

    def _reset_timeout(self, timeout: int = None):
        if self.test_timeout_task:
            self.test_timeout_task.cancel()
        self.test_timeout_task = asyncio.create_task(
            self._test_timeout_trigger(timeout)
        )

    # Must be called in another thread to avoid blocking the async loop
    def _sync_write_log(
        self, test_name: str, stdout_lines: list[str], stderr_blocks: list[bytes]
    ):
        with self.ctx.sync_stdout_writer_lock:
            self.ctx.sync_stdout_writer.write(b"===================================\n")
            self.ctx.sync_stdout_writer.write(f"TestCase: {test_name}\n".encode())
            self.ctx.sync_stdout_writer.write("\n".join(stdout_lines).encode())
            self.ctx.sync_stdout_writer.write(b"\n\n")
        with self.ctx.sync_stderr_writer_lock:
            self.ctx.sync_stderr_writer.write(b"===================================\n")
            self.ctx.sync_stderr_writer.write(f"TestCase: {test_name}\n".encode())
            self.ctx.sync_stderr_writer.write(b"".join(stderr_blocks))
            self.ctx.sync_stderr_writer.write(b"\n\n")

    def _prefix(self):
        return f"{self.ctx.finished_tests}/{self.ctx.all_tests}".ljust(12)

    def _elapsed(self, s: TestStatus, is_timedout: bool = False) -> str:
        if s.start_at == 0 or s.end_at == 0:
            return ""
        ms = int((s.end_at - s.start_at) * 1000)
        if is_timedout:
            return f" {C.TIME_WARN}{ms}ms(timedout){C.RESET}"
        else:
            color = C.TIME_OK if ms < 1000 else C.TIME_WARN
            return f" {color}{ms}ms{C.RESET}"

    def _on_test_run(self, test_name: str):
        s = self.status[test_name]
        s.result = TestStatus.Result.NOT_RUN
        s.start_at = time.time()
        s.n_runs += 1
        self.current_test_name = test_name
        self.current_logs_stdout = []
        self.current_logs_stderr = []
        self.current_logs_stderr_size = 0

        # Use a smaller timeout for the first run,
        # if we don't know how long it will take.
        timeout = self.ctx.options.v.timeout
        if s.n_runs == 1:
            new_timeout = timeout // 3
            history_ms = self.ctx.history.get_ms(self.remaining_tests.binary, test_name)
            if history_ms < new_timeout * 1000 or history_ms > timeout * 1000:
                timeout = new_timeout
        self._reset_timeout(timeout)

    def _record_test_elapsed_history(self, test_name: str, allow_shorter: bool = False):
        s = self.status[test_name]
        if s.start_at == 0 or s.end_at == 0:
            return
        ms = int((s.end_at - s.start_at) * 1000)

        binary = self.remaining_tests.binary
        if binary not in self.ctx.history:
            self.ctx.history[binary] = {}
        history_b = self.ctx.history[binary]
        if test_name in history_b and ms < history_b[test_name] and not allow_shorter:
            return
        history_b[test_name] = ms

    def _on_test_run_finished(self, test_name: str):
        s = self.status[test_name]
        s.end_at = time.time()
        self.current_logs_stdout = []
        self.current_logs_stderr = []
        self.current_logs_stderr_size = 0
        self.current_test_name = ""
        self.remaining_tests.tests.discard(test_name)
        if len(self.remaining_tests.tests) == 0:
            # Sometimes process may stuck even after a test is finished
            self._reset_timeout(1)
        else:
            # A test is finished but next task is not started yet
            self._reset_timeout(10)

    def _on_test_pass(self, test_name: str):
        s = self.status[test_name]
        s.result = TestStatus.Result.PASS
        self._on_test_run_finished(test_name)
        self._record_test_elapsed_history(test_name, allow_shorter=True)
        self.ctx.finished_tests += 1
        print(f"{self._prefix()}{Out.OK}{test_name}{self._elapsed(s)}")

    def _on_test_fail(self, test_name: str):
        s = self.status[test_name]
        s.result = (
            TestStatus.Result.FAIL
            if not self.has_timed_out
            else TestStatus.Result.FAIL_TIMEOUT
        )
        self.log_writer_tasks.append(
            asyncio.create_task(
                asyncio.to_thread(
                    self._sync_write_log,
                    test_name,
                    self.current_logs_stdout,
                    self.current_logs_stderr,
                )
            )
        )
        self._on_test_run_finished(test_name)
        self._record_test_elapsed_history(test_name, allow_shorter=False)

        # Allow failed tests to rerun several times (and in a standalone proc).
        should_retry = False
        if not test_name in self.ctx.options.suppress_tests:
            if self.has_timed_out and s.n_runs <= 1:
                should_retry = True  # Timeout task only retry once
            elif (not self.has_timed_out) and s.n_runs < 3:
                should_retry = True
        if not should_retry:
            self.ctx.finished_tests += 1

        is_suppressed = test_name in self.ctx.options.suppress_tests

        plabel = Out.FAILED
        if is_suppressed or should_retry:
            plabel = Out.FAILED_DIM
        psuppress = " [suppressed]" if is_suppressed else ""
        pretry = " will retry..." if should_retry else ""
        pattempt = f" [run#{s.n_runs}]" if s.n_runs > 1 else ""
        print(
            f"{self._prefix()}{plabel}{test_name}{self._elapsed(s, self.has_timed_out)}"
            f"{psuppress}{pattempt}{pretry}"
        )

        if should_retry:
            duration = self.ctx.history.get_ms(self.remaining_tests.binary, test_name)
            self.ctx.scheduler.create_task(
                priority=-duration * 10,  # Single retry has higher priority
                coro=spawn_test_proc(
                    self.ctx,
                    TestGroup(self.remaining_tests.binary, set([test_name])),
                ),
            )

    def _on_proc_exit(self):
        if self.current_test_name:
            self._on_test_fail(self.current_test_name)
        # There may be tests not run yet due to proc crash. For these tests, we run them in a new proc.
        rerun_tests = set()
        for test_name in self.remaining_tests.tests:
            assert self.status[test_name].result == TestStatus.Result.NOT_RUN
            rerun_tests.add(test_name)
        durations = sum(
            self.ctx.history.get_ms(self.remaining_tests.binary, test_name)
            for test_name in rerun_tests
        )
        if rerun_tests:
            self.ctx.scheduler.create_task(
                priority=-durations,
                coro=spawn_test_proc(
                    self.ctx,
                    TestGroup(self.remaining_tests.binary, rerun_tests),
                ),
            )

    async def _test_timeout_trigger(self, timeout: int = None):
        if timeout is None:
            timeout = self.ctx.options.v.timeout
        await asyncio.sleep(timeout)
        self.has_timed_out = True
        try:
            self.proc.kill()
        except ProcessLookupError:
            pass


def default_options_parser() -> optparse.OptionParser:
    # fmt: off
    parser = optparse.OptionParser(usage="usage: %prog [options] binary [binary ...] -- [additional args]")
    parser.add_option("--working_dir", type="string", default="./gtest_10x_workdir", help="specify the CWD for running the test and where to output log files")
    parser.add_option("-w", "--workers", type="int", default=os.cpu_count(), help="number of workers to spawn")
    parser.add_option("--shard_count", type="int", default=1, help="total number of shards (for sharding test execution between multiple machines)")
    parser.add_option("--shard_index", type="int", default=0, help="zero-indexed number identifying this shard (for sharding test execution between multiple machines)")
    parser.add_option("--skip_list", type="string", default="", help="skip running these tests in the file")
    parser.add_option("--suppress_list", type="string", default="", help="suppress failures caused by these tests in the file")
    parser.add_option("--timeout", type="int", default=60, help="interrupt current test after specified timeout (in seconds)")
    # fmt: on
    return parser


async def find_tests(ctx: GlobalContext) -> list[TestCase]:
    tests_n = 0  # Include discarded tests
    tests = []
    for test_binary in ctx.options.binaries:
        command = [test_binary] + ctx.options.additional_args + ["--gtest_list_tests"]
        proc = await asyncio.create_subprocess_exec(
            *command,
            limit=512 * 1024,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
            cwd=ctx.options.v.working_dir_tests,
        )

        test_group = ""
        while not proc.stdout.at_eof():
            data = await proc.stdout.readline()
            line = data.decode("utf-8").rstrip()
            if not line.strip():
                continue
            if line[0] != " ":
                test_group = line.split("#")[0].strip()
                continue
            line = line.split("#")[0].strip()
            if not line:
                continue

            test_name = test_group + line
            tests_n += 1

            if "DISABLED_" in test_name:
                continue
            if (tests_n - ctx.options.v.shard_index) % ctx.options.v.shard_count != 0:
                continue
            if test_name in ctx.options.skip_tests:
                print(f"        {Out.SKIPPED}{test_name}")
                continue

            tests.append(
                TestCase(
                    test_binary,
                    test_group,
                    test_name,
                    ctx.history.get_ms(test_binary, test_name),
                )
            )
            ctx.all_tests += 1

    return tests


async def execute_tests(ctx: GlobalContext, tests: list[TestCase]):
    @dataclasses.dataclass
    class HeapItem:
        total_duration: int
        test_cases: list[TestCase]

        def __lt__(self, other):
            return self.total_duration < other.total_duration

    tests.sort(key=lambda x: x.last_duration, reverse=True)

    # Distribute all tests to n workers in a pre-calculated way. If we have 5 workers, we will generate 5*10 tasks.
    # In this way, if a worker finish tests early it will take over more tests to work with.
    heap: list[HeapItem] = []
    worker_factor = 10
    for _ in range(ctx.options.v.workers * worker_factor):
        heapq.heappush(heap, HeapItem(0, []))
    for test in tests:
        item = heapq.heappop(heap)
        item.total_duration += ctx.history.get_ms(test.binary, test.full_test_name)
        item.test_cases.append(test)
        heapq.heappush(heap, item)

    # Even if we have a priority scheduler, it schedules tasks in sync when there is no wait
    # so that we still need to sort by priority here.
    heap.sort(key=lambda x: x.total_duration, reverse=True)
    for item in heap:
        if len(item.test_cases) == 0:
            continue
        # Group tests in the same binary together
        groups_by_binary: dict[str, TestGroup] = {}
        durations_by_binary: dict[str, int] = {}
        for test_case in item.test_cases:
            if test_case.binary not in groups_by_binary:
                groups_by_binary[test_case.binary] = TestGroup(test_case.binary, set())
                durations_by_binary[test_case.binary] = 0
            groups_by_binary[test_case.binary].tests.add(test_case.full_test_name)
            durations_by_binary[test_case.binary] += test_case.last_duration
        for binary, test_group in groups_by_binary.items():
            durations = durations_by_binary[binary]
            ctx.scheduler.create_task(
                priority=-durations,
                coro=spawn_test_proc(ctx, test_group),
            )

    await ctx.scheduler.wait_all()


async def spawn_test_proc(ctx: GlobalContext, tests: TestGroup):
    # Use a clean and isolated working directory for each process
    # (similar to gtest_parallel's --serialize_test_cases)
    ctx.spawned_procs += 1
    proc_wd = os.path.join(ctx.options.v.working_dir_tests, f"proc_{ctx.spawned_procs}")
    await asyncio.to_thread(
        lambda: os.makedirs(
            proc_wd,
            exist_ok=True,
        )
    )

    watcher = ProcWatcher(ctx, tests)
    command = [tests.binary] + ctx.options.additional_args
    command.append("--gtest_filter=" + ":".join(list(tests.tests)))
    proc = await asyncio.create_subprocess_exec(
        *command,
        stderr=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        cwd=proc_wd,
    )
    await watcher.handle_proc(proc)


def sync_read_test_list_file(file_path: str) -> set[str]:
    l = set()
    with open(file_path, "r", encoding="utf-8") as f:
        while line := f.readline():
            # Remove anything after #
            line = line.split("#")[0].strip()
            if line:
                l.add(line)
    return l


# fmt: off
def summarize(ctx: GlobalContext) -> int:
    names: dict[TestStatus.Result, list[str]] = {
        TestStatus.Result.PASS: [],
        TestStatus.Result.FAIL: [],
        TestStatus.Result.FAIL_TIMEOUT: [],
        TestStatus.Result.PASS_FLAKY: [],
        TestStatus.Result.FAIL_SUPPRESSED: [],
    }

    for status_by_binary in ctx.status_repo.values():
        for test_name, status in status_by_binary.items():
            assert status.result != TestStatus.Result.NOT_RUN
            if status.result == TestStatus.Result.PASS and status.n_runs > 1:
                names[TestStatus.Result.PASS_FLAKY].append(test_name)
            elif status.result != TestStatus.Result.PASS and test_name in ctx.options.suppress_tests:
                names[TestStatus.Result.FAIL_SUPPRESSED].append(test_name)
            else:
                names[status.result].append(test_name)

    print("\n\nTest finished.")

    n_pass_flaky = len(names[TestStatus.Result.PASS_FLAKY])
    n_pass = len(names[TestStatus.Result.PASS]) + n_pass_flaky
    n_failed = len(names[TestStatus.Result.FAIL]) + len(names[TestStatus.Result.FAIL_TIMEOUT])
    n_failed_suppressed = len(names[TestStatus.Result.FAIL_SUPPRESSED])

    print(
        f"{C.OK}{n_pass}{C.RESET} tests passed ({n_pass_flaky} flaky tests), "
        f"{C.FAIL}{n_failed}{C.RESET} tests failed, "
        f"{C.FAIL}{n_failed_suppressed}{C.RESET} test failures are suppressed."
    )

    if n_failed_suppressed > 0:
        print(f"\n{C.WARN}Suppressed failed tests:{C.RESET}")
        for test_name in sorted(names[TestStatus.Result.FAIL_SUPPRESSED]):
            print(f"  {test_name}")

    if n_pass_flaky > 0:
        print(f"\n{C.WARN}Flaky tests:{C.RESET}")
        for test_name in sorted(names[TestStatus.Result.PASS_FLAKY]):
            print(f"  {test_name}")

    if n_failed > 0:
        print(f"\n{C.FAIL}Failed tests:{C.RESET}")
        for test_name in sorted(names[TestStatus.Result.FAIL]):
            print(f"  {test_name}")
        for test_name in sorted(names[TestStatus.Result.FAIL_TIMEOUT]):
            print(f"  {test_name} (timedout)")
        return 1

    return 0
# fmt: on


async def main():
    options = Options()

    # Remove additional arguments (anything after --)
    for i, arg in enumerate(sys.argv):
        if arg == "--":
            options.additional_args = sys.argv[i + 1 :]
            sys.argv = sys.argv[:i]
            break

    parser = default_options_parser()
    options.v, options.binaries = parser.parse_args()

    if not options.binaries:
        parser.print_usage()
        sys.exit(1)

    unique_binaries = set(os.path.basename(binary) for binary in options.binaries)
    assert len(unique_binaries) == len(
        options.binaries
    ), "All test binaries must have an unique basename."

    # Resolve into absolute path because we will change the working directory when running the tests.
    for binary in options.binaries:
        assert os.path.exists(binary), f"Binary not found: {binary}"
    options.binaries = [os.path.abspath(binary) for binary in options.binaries]

    options.v.working_dir_tests = os.path.join(options.v.working_dir, "./tests_tmp")
    if os.path.exists(options.v.working_dir_tests):
        shutil.rmtree(options.v.working_dir_tests)
    os.makedirs(options.v.working_dir_tests, exist_ok=True)

    history_path = os.path.join(options.v.working_dir, "history.json")

    if options.v.skip_list:
        options.skip_tests = sync_read_test_list_file(options.v.skip_list)
    if options.v.suppress_list:
        options.suppress_tests = sync_read_test_list_file(options.v.suppress_list)

    stdout_file = os.path.join(options.v.working_dir, "test_stdout.log")
    stderr_file = os.path.join(options.v.working_dir, "test_stderr.log")
    print("Test case failures will be log to:")
    print(f"  {stdout_file}")
    print(f"  {stderr_file}")
    print()
    print(
        "Start running tests. Slow tests will be run first, fast tests will be run later. Keep patient..."
    )

    exit_code = 1
    with open(stdout_file, "wb") as stdout_writer:
        with open(stderr_file, "wb") as stderr_writer:
            ctx = GlobalContext(
                scheduler=Scheduler(max_concurrent=options.v.workers),
                status_repo=TestStatusRepo(),
                history=History.from_file(history_path),
                sync_stdout_writer=stdout_writer,
                sync_stderr_writer=stderr_writer,
                options=options,
            )
            try:
                tests = await find_tests(ctx)
                await execute_tests(ctx, tests)
                # Summarize test results
                exit_code = summarize(ctx)
                if exit_code > 0:
                    print(f"\n{C.FAIL}TEST FAILED.{C.RESET}")
                    print(f"Failure details: {stdout_file}")
                    print(f"Failure logs:    {stderr_file}")
                else:
                    print(f"\n{C.OK}TESTS PASSED.{C.RESET}")
            except asyncio.CancelledError:
                print(f"{C.FAIL}Interrupted by user.{C.RESET}")
            finally:
                # Always persist a history even if user interrupt the process
                ctx.history.persist(history_path)
    return exit_code


if __name__ == "__main__":
    # Python 3.9: built-in collection typing
    assert sys.version_info >= (3, 9), "Python 3.9 or later is required."
    exit(asyncio.run(main()))
