#!/usr/bin/python3

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
