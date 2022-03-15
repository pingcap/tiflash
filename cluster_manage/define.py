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


TIFLASH = 'tiflash'
TIFLASH_LABEL = {'key': 'engine', 'value': TIFLASH}
REGION_COUNT = 'region_count'
TIFLASH_REGION_COUNT = 'flash_region_count'
LOCATION_LABELS = 'location_labels'
REPLICA_COUNT = 'replica_count'
LEARNER = 'learner'
AVAILABLE = 'available'
PRIORITY = 'high_priority'
TIFLASH_CLUSTER_MUTEX_KEY = '/{}/cluster/leader'.format(TIFLASH)
TIFLASH_CLUSTER_HTTP_PORT = '/{}/cluster/http_port/'.format(TIFLASH)
LABEL_CONSTRAINTS = 'label_constraints'
DDL_GLOBAL_SCHEMA_VERSION = '/tidb/ddl/global_schema_version'
TIFLASH_LAST_HANDLED_SCHEMA_VERSION = '/{}/cluster/last_handled_schema_version'.format(TIFLASH)
TIFLASH_LAST_HANDLED_SCHEMA_VERSION_TSO_SPLIT = b'_tso_'
TIFLASH_LAST_HANDLED_SCHEMA_TIME_OUT = 300
