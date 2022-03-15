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


import define

TIFLASH_GROUP_ID = define.TIFLASH
DEFAULT_LABEL_CONSTRAINTS = [{"key": "engine", "op": "in", "values": [define.TIFLASH]}]

base_rule = {
    "group_id": TIFLASH_GROUP_ID,
    'id': '',
    "index": 0,
    "override": True,
    "start_key": None,
    "end_key": None,
    "role": define.LEARNER,
    "count": 2,
    define.LABEL_CONSTRAINTS: DEFAULT_LABEL_CONSTRAINTS,
    define.LOCATION_LABELS: None
}


class PlacementRule:
    def __init__(self, **entries):
        self.__dict__.update(entries)
        if not hasattr(self, define.LOCATION_LABELS):
            self.location_labels = []


def make_rule(rid: str, start_key, end_key, count, location_labels):
    rule = PlacementRule(**base_rule)
    rule.id = rid
    rule.start_key = start_key
    rule.end_key = end_key
    rule.count = count
    rule.location_labels = location_labels
    return rule
