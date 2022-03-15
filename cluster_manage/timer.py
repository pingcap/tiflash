#!/usr/bin/python3
# Copyright 2022 PingCAP, Ltd.
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


import heapq
import time


class Task:
    def __init__(self, func, run_tso):
        self.func = func
        self.run_tso = run_tso

    def __lt__(self, other):
        return self.run_tso < other.run_tso

    def __ge__(self, other):
        return self.run_tso >= other.run_tso

    def __le__(self, other):
        return self.run_tso <= other.run_tso


class Timer:
    def __init__(self):
        self.queue = []

    def add(self, func, interval):
        heapq.heappush(self.queue, Task(func, time.time() + interval))

    def run(self):
        while True:
            if not len(self.queue):
                time.sleep(0.2)
                continue
            task = heapq.heappop(self.queue)
            now = time.time()
            if now < task.run_tso:
                time.sleep(task.run_tso - now)
            task.func()


def main():
    timer = Timer()

    def run():
        print('x')
        timer.add(run, time.time() + 2)

    timer.add(run, time.time() + 2)
    timer.run()


if __name__ == '__main__':
    main()
