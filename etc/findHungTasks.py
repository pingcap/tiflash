# Copyright 2023 PingCAP, Ltd.
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

import re

# How to use?
#   1. Collect logs that printed at the beginning of the task.
#        such as: cat tiflash.log | grep task\ starts\ preprocessing > task_start_file
#   2. Collect logs that printed at the end of the task.
#        such as: cat tiflash.log | grep task\ ends,\ time\ cost\ is > task_end_file
#   3. assign the above two file name to the corresponding global variable.
#        global variable that needs assigning: task_start_file and task_finish_file

task_start_file = "" # file containing logs that printed at the beginning of the task
task_finish_file = "" # file containing logs that printed at the end of the task

# key: start_ts
# value: task id list
all_finished_tasks = {}
hung_tasks = {}

pattern = "start_ts:([0-9]+)>,task_id:([0-9]+)>"

def analyzeLogImpl(pat, log):
    res = pat.search(log)
    if len(res.groups()) != 2:
        print(log)
        print(res)
        raise Exception("Invalid log")
    start_ts = res.group(1)
    task_id = res.group(2)
    return [start_ts, task_id]


def analyzeLogAndGetFinishedTask(pat, log):
    global all_finished_tasks

    res = analyzeLogImpl(pat, log)
    start_ts = res[0]
    task_id = res[1]

    if start_ts in all_finished_tasks:
        all_finished_tasks[start_ts].append(task_id)
    else:
        all_finished_tasks[start_ts] = [task_id]


def addHungTask(start_ts, task_id):
    global hung_tasks
    if start_ts in hung_tasks:
        hung_tasks[start_ts].append(task_id)
    else:
        hung_tasks[start_ts] = [task_id]


def analyzeLogAndGetHungTask(pat, log):
    global all_finished_tasks
    global hung_tasks

    res = analyzeLogImpl(pat, log)
    start_ts = res[0]
    task_id = res[1]

    # Check if the start task is in finished tasks
    is_found = False
    if start_ts in all_finished_tasks:
        finished_tasks = all_finished_tasks[start_ts]
        for finished_task_id in finished_tasks:
            if finished_task_id == task_id:
                is_found = True
                break

    if not is_found:
        addHungTask(start_ts, task_id)


def collectFinishedTasks(pat):
    global task_finish_file
    with open(task_finish_file, 'r') as f:
        logs = f.readlines()
        for log in logs:
            analyzeLogAndGetFinishedTask(pat, log)


# Now, we get finished tasks, then check if the start task
# is in the finished tasks. If no, the task must be hung.
def findHungTasksImpl(pat):
    global task_start_file
    with open(task_start_file, 'r') as f:
        logs = f.readlines()
        for log in logs:
            analyzeLogAndGetHungTask(pat, log)


def printHungTasks():
    global hung_tasks

    print("Hung tasks:")
    for start_ts, tasks_id in hung_tasks.items():
        for task_id in tasks_id:
            print("start_ts: %s, task_id %s" % (start_ts, task_id))


def findHungTasks():
    global pattern
    pat = re.compile(pattern)

    collectFinishedTasks(pat)
    findHungTasksImpl(pat)
    printHungTasks()


def check():
    global task_start_file
    global task_finish_file
    if len(task_start_file) == 0 or len(task_finish_file) == 0:
        raise Exception("Please designate the files that contain logs")


if __name__ == "__main__":
    check()
    findHungTasks()
