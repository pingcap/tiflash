#!/usr/bin/python3

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
