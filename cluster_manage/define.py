#!/usr/bin/python3

TIFLASH = 'tiflash'
TIFLASH_REPLICA = 'tiflash_replica'
TIFLASH_LABEL = {'key': 'engine', 'value': TIFLASH}
METRICS_UPDATE_TIME = '{}/metrics_update_time'.format(TIFLASH)
REGION_COUNT = 'region_count'
TIFLASH_REGION_COUNT = 'flash_region_count'
LOCATION_LABELS = 'location_labels'
REPLICA_COUNT = 'replica_count'
LEARNER = 'learner'
AVAILABLE = 'available'
REPLICA_SYNC = '{}_sync_rate'.format(TIFLASH_REPLICA)
REPLICA_SYNC_DISC = 'TiFlash replica syncing rate of table 0.0 ~ 1.0'
TABLE_ID = 'table_id'
STATISTIC_INFO = '{}_statistic'.format(TIFLASH_REPLICA)
STATISTIC_INFO_DISC = 'Information about statistic'
LEADER_ADDR = 'leader_addr'
TOTAL_TABLE_CNT = 'total_table_cnt'
UNAVAILABLE_TABLE_CNT = 'unavailable_table_cnt'
TIFLASH_REPLICA_STATUS = '{}_info'.format(TIFLASH_REPLICA)
TIFLASH_CLUSTER_MUTEX_KEY = '{}/cluster/leader'.format(TIFLASH)
LABEL_CONSTRAINTS = 'label_constraints'
