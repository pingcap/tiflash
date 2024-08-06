#!/usr/bin/env python3
# Copyright 2023 PingCAP, Inc.
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

import sys
import json
import logging

def main():
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y/%m/%d %H:%M:%S', level=logging.INFO)
    if len(sys.argv) < 2:
        logging.error("Usage: {} path/to/grafana.json".format(sys.argv[0]))
        return 1

    infile = sys.argv[1]
    j = json.load(open(infile))
    panels = j['panels']
    existing_ids = {}

    ok = True

    if j["title"] != "Test-Cluster-TiFlash-Summary":
        ok = False
        logging.error("key={} actual={}".format("title", j["title"]))
    if j["uid"] != "SVbh2xUWk":
        ok = False
        logging.error("key={} actual={}".format("uid", j["uid"]))
    if j["__inputs"][0]["name"] != "DS_TEST-CLUSTER" or j["__inputs"][0]["label"] != "Test-Cluster":
        ok = False
        logging.error("key={} actual={}".format("__inputs", j["__inputs"]))

    for panel in panels:
        ok &= handle_panel(panel, j["title"], existing_ids)
    if not ok:
        logging.error("!!!Error detected!!!")
        return 2
    else:
        logging.info("All checks passed")
        return 0

def handle_panel(panel, parent_title, existing_ids):
    logging.debug('title: {} type: {}'.format(parent_title, panel["type"]))
    if panel["type"] == "row":
        # Check sub panels recursively
        logging.info("Checking panels under row '{}'".format(panel["title"]))
        sub_panels = panel["panels"]
        ok = True
        for p in sub_panels:
            ok &= handle_panel(p, panel["title"], existing_ids)
        logging.info("Check done for panels under row '{}'".format(panel["title"]))
        return ok

    # Check whether there exist duplicate id
    if panel["id"] not in existing_ids:
        existing_ids[panel["id"]] = {
            "panel": panel,
            "parent_title": parent_title,
        }
        return True
    else:
        current_title = panel["title"]
        existing_panel = existing_ids[panel["id"]]
        logging.error("Found duplicated id {}".format(panel["id"]))
        logging.error("Original panel title: '{}' under row '{}'".format(existing_panel["panel"]["title"], existing_panel["parent_title"]))
        logging.error("Duplicate panel title: '{}' under row '{}'".format(current_title, parent_title))
        return False


if __name__ == '__main__':
    sys.exit(main())
