// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Memory trace');

local number_of_KeyspacesP = graphPanel.new(
  title='Number of Keyspaces',
  datasource=common.datasource,
  formatY1='short',
  formatY2='s',
  fill=1,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_NumKeyspace{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='keyspace-{{instance}}',
    intervalFactor=1,
  )
);

local number_of_Physical_TablesP = graphPanel.new(
  title='Number of Physical Tables',
  datasource=common.datasource,
  formatY1='short',
  formatY2='s',
  fill=1,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_NumStorageDeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='tables-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_NumIStorage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='tables-all-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
);

local number_of_SegmentsP = graphPanel.new(
  title='Number of Segments',
  datasource=common.datasource,
  formatY1='short',
  formatY2='s',
  fill=1,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_NumSegment{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='segments-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_NumMemTable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='mem_table-{{instance}}',
    intervalFactor=1,
  )
);

local bytes_of_MemTablesP = graphPanel.new(
  title='Bytes of MemTables',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='s',
  fill=1,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_BytesMemTable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='bytes-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_BytesMemTableAllocated{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='bytes-allocated-{{instance}}',
    intervalFactor=1,
  )
);

local mark_Cache_and_Minmax_Index_Cache_Memory_UsageP = graphPanel.new(
  title='Mark Cache and Minmax Index Cache Memory Usage',
  datasource=common.datasource,
  description='The memory usage of mark cache and minmax index cache',
  formatY1='bytes',
  formatY2='short',
  min='0',
  fill=0,
  nullPointMode='null',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
  legend_sideWidth=250,
)
.addTarget(
  prometheus.target(
    'tiflash_system_asynchronous_metric_MarkCacheBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='mark_cache_{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_asynchronous_metric_MinMaxIndexFiles{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='minmax_index_cache_{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_asynchronous_metric_RNMVCCIndexCacheBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='rn_mvcc_index_cache_{{instance}}',
  )
)
.addSeriesOverride({ alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 });

local effectiveness_of_Mark_CacheP = graphPanel.new(
  title='Effectiveness of Mark Cache',
  datasource=common.datasource,
  description='cache misses or cache hits of mark_cache.\nBased on this infactor, we can check whether mark_cache is large enough',
  formatY1='percentunit',
  formatY2='percent',
  fill=1,
  nullPointMode='null',
  pointradius=2,
)
.addTarget(
  prometheus.target(
    'max(tiflash_system_profile_event_MarkCacheMisses{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='mark cache misses',
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_system_profile_event_MarkCacheHits{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='mark cache hits',
  )
);

local schema_of_Column_FileP = graphPanel.new(
  title='Schema of Column File',
  datasource=common.datasource,
  description='Information about schema of column file, to learn the memory usage of schema',
  formatY1='short',
  formatY2='short',
  fill=1,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'max(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"current_size"}) by (instance)',
    legendFormat='current_size-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"hit_count"}[1m])) by (instance)',
    legendFormat='hit_count_ops-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"still_used_when_evict"}) by (instance)',
    legendFormat='still_used_when_evict-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"miss_count"}[1m])) by (instance)',
    legendFormat='miss_count_ops-{{instance}}',
  )
);

local read_SnapshotsP = graphPanel.new(
  title='Read Snapshots',
  datasource=common.datasource,
  formatY1='short',
  formatY2='s',
  min='0',
  fill=1,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_SegmentReadTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='read_tasks-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_asynchronous_metric_MaxDTDeltaOldestSnapshotLifetime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='max_snapshot_lifetime-{{instance}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/max_snapshot_lifetime/', yaxis: 2 });

local memory_by_threadP = graphPanel.new(
  title='Memory by thread',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
  fill=1,
  nullPointMode='null',
  pointradius=2,
)
.addTarget(
  prometheus.target(
    'rate(tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}[$__interval])',
    legendFormat='{{instance}}-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    '-rate(tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}[$__interval])',
    legendFormat='{{instance}}-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}',
    legendFormat='{{instance}}-{{type}}-tot',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    '-tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}',
    legendFormat='{{instance}}-{{type}}-tot',
    hide=true,
  )
);

local memory_by_thread_proxyP = graphPanel.new(
  title='Memory by thread (proxy)',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
  fill=1,
  nullPointMode='null',
  pointradius=2,
  legend_hideEmpty=true,
  legend_hideZero=true,
)
.addTarget(
  prometheus.target(
    'rate(tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}[$__interval])',
    legendFormat='{{instance}}-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    '-rate(tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}[$__interval])',
    legendFormat='{{instance}}-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}',
    legendFormat='{{instance}}-{{type}}-tot',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    '-tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}',
    legendFormat='{{instance}}-{{type}}-tot',
    hide=true,
  )
);

local memory_by_classP = graphPanel.new(
  title='Memory by class',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
  fill=1,
  nullPointMode='null',
  pointradius=2,
)
.addTarget(
  prometheus.target(
    'tiflash_memory_usage_by_class{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='{{instance}}-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'rate(tiflash_memory_usage_by_class{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__interval])',
    legendFormat='{{instance}}-{{type}}-rate',
    hide=true,
  )
);

local kVStore_memoryP = graphPanel.new(
  title='KVStore memory',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
  fill=1,
  nullPointMode='null',
  pointradius=2,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_MemoryTrackingKVStore{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='{{instance}}',
  )
);


{
  row: rowObj
  .addPanel(number_of_KeyspacesP, gridPos=common.pos(12, 8, x=0, y=40))
  .addPanel(number_of_Physical_TablesP, gridPos=common.pos(12, 8, x=12, y=40))
  .addPanel(number_of_SegmentsP, gridPos=common.pos(12, 8, x=0, y=48))
  .addPanel(bytes_of_MemTablesP, gridPos=common.pos(12, 8, x=12, y=48))
  .addPanel(mark_Cache_and_Minmax_Index_Cache_Memory_UsageP, gridPos=common.pos(12, 8, x=0, y=56))
  .addPanel(effectiveness_of_Mark_CacheP, gridPos=common.pos(12, 8, x=12, y=56))
  .addPanel(schema_of_Column_FileP, gridPos=common.pos(12, 8, x=0, y=64))
  .addPanel(read_SnapshotsP, gridPos=common.pos(12, 8, x=12, y=64))
  .addPanel(memory_by_threadP, gridPos=common.pos(12, 8, x=0, y=72))
  .addPanel(memory_by_thread_proxyP, gridPos=common.pos(12, 8, x=12, y=72))
  .addPanel(memory_by_classP, gridPos=common.pos(12, 8, x=0, y=80))
  .addPanel(kVStore_memoryP, gridPos=common.pos(12, 8, x=12, y=80))
  ,
  panels: [
    { panel: number_of_KeyspacesP, w: 12, h: 8, x: 0, y: 40 },
    { panel: number_of_Physical_TablesP, w: 12, h: 8, x: 12, y: 40 },
    { panel: number_of_SegmentsP, w: 12, h: 8, x: 0, y: 48 },
    { panel: bytes_of_MemTablesP, w: 12, h: 8, x: 12, y: 48 },
    { panel: mark_Cache_and_Minmax_Index_Cache_Memory_UsageP, w: 12, h: 8, x: 0, y: 56 },
    { panel: effectiveness_of_Mark_CacheP, w: 12, h: 8, x: 12, y: 56 },
    { panel: schema_of_Column_FileP, w: 12, h: 8, x: 0, y: 64 },
    { panel: read_SnapshotsP, w: 12, h: 8, x: 12, y: 64 },
    { panel: memory_by_threadP, w: 12, h: 8, x: 0, y: 72 },
    { panel: memory_by_thread_proxyP, w: 12, h: 8, x: 12, y: 72 },
    { panel: memory_by_classP, w: 12, h: 8, x: 0, y: 80 },
    { panel: kVStore_memoryP, w: 12, h: 8, x: 12, y: 80 }
  ],
}
