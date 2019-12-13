#!/usr/bin/python3

import util

PR_FLASH_GROUP = "tiflash"

base_rule = {
    "group_id": PR_FLASH_GROUP,
    "id": "",
    "index": 0,
    "override": True,
    "start_key": None,
    "end_key": None,
    "role": "learner",
    "count": 2,
    "label_constraints": [
        {"key": "engine", "op": "in", "values": ["tiflash"]}
    ],
    "location_labels": None
}


class PlacementRule:
    def __init__(self, **entries):
        self.__dict__.update(entries)
        if not hasattr(self, 'location_labels'):
            self.location_labels = []


def make_rule(rid: str, start_key, end_key, count, location_labels):
    rule = PlacementRule(**base_rule)
    rule.id = rid
    rule.start_key = start_key
    rule.end_key = end_key
    rule.count = count
    rule.location_labels = location_labels
    return rule


def get_group_rules(group="tiflash"):
    return []


def main():
    rule = make_rule("1", b'1', b'2', 2, ['host'])
    print(util.obj_2_dict(rule))


if __name__ == '__main__':
    main()
