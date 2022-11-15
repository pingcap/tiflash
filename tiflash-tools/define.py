#!/usr/bin/python3

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
