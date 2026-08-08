// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Disaggregated-Compute');

local read_Duration_BreakdownP = graphPanel.new(
  title='Read Duration Breakdown',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_disaggregated_breakdown_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, $additional_groupby))',
    legendFormat='99%-{{type}} {{$additional_groupby}}',
  )
)
.resetYaxes()
.addYaxis(
  format='s',
  min='0',
)
.addYaxis(
  format='short',
);

local remote_Cache_OperationsP = graphPanel.new(
  title='Remote Cache Operations',
  datasource=common.datasource,
  description='Remote Cache Operations',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dtfile_hit"}[1m]))/sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dtfile_hit|dtfile_miss"}[1m]))',
    legendFormat='dtfile_cache_hit_ratio',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"page_hit"}[1m]))/sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"page_hit|page_miss"}[1m]))',
    legendFormat='page_cache_hit_ratio',
  )
)
.addSeriesOverride({ alias: 'dtfile_cache_hit_ratio', yaxis: 2 })
.addSeriesOverride({ alias: 'page_cache_hit_ratio', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
);

local remote_Cache_FlowP = graphPanel.new(
  title='Remote Cache Flow',
  datasource=common.datasource,
  description='Remote Cache Flow',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_remote_cache_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
  show=false,
);

local remote_Cache_BG_Download_DurationP = graphPanel.new(
  title='Remote Cache BG Download Duration',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, stage, file_type, $additional_groupby))',
    legendFormat='999%-{{stage}}-{{file_type}} {{$additional_groupby}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, stage, file_type, $additional_groupby))',
    legendFormat='99%-{{stage}}-{{file_type}} {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    '(sum(rate( tiflash_storage_remote_cache_bg_download_stage_seconds_sum {k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} [$__rate_interval] )) by (stage, file_type, $additional_groupby) / sum(rate( tiflash_storage_remote_cache_bg_download_stage_seconds_count {k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} [$__rate_interval] )) by (stage, file_type, $additional_groupby) )',
    legendFormat='avg-{{stage}}-{{file_type}} {{$additional_groupby}}',
  )
)
.resetYaxes()
.addYaxis(
  format='s',
  min='0',
)
.addYaxis(
  format='short',
);

local remote_Cache_Wait_on_Downloading_DurationP = graphPanel.new(
  title='Remote Cache Wait on Downloading Duration',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, result, file_type, $additional_groupby))',
    legendFormat='999%-{{result}}-{{file_type}} {{$additional_groupby}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, result, file_type, $additional_groupby))',
    legendFormat='99%-{{result}}-{{file_type}} {{$additional_groupby}}',
  )
)
.resetYaxes()
.addYaxis(
  format='s',
  min='0',
)
.addYaxis(
  format='short',
);

local remote_Cache_Wait_on_Downloading_OPSP = graphPanel.new(
  title='Remote Cache Wait on Downloading OPS',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_remote_cache_wait_on_downloading_result{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (result, file_type , $additional_groupby)',
    legendFormat='{{result}}-{{file_type}} {{$additional_groupby}}',
  )
)
.addSeriesOverride({ alias: '', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
  decimals=0,
)
.addYaxis(
  format='s',
);

local remote_Cache_Wait_on_Downloading_FlowP = graphPanel.new(
  title='Remote Cache Wait on Downloading Flow',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_remote_cache_wait_on_downloading_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (result, file_type , $additional_groupby)',
    legendFormat='{{result}}-{{file_type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
  show=false,
);

local remote_Cache_GaugeP = graphPanel.new(
  title='Remote Cache Gauge',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_storage_remote_cache_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type, instance)',
    legendFormat='{{type}}-{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='short',
  decimals=0,
)
.addYaxis(
  format='short',
);

local remote_Cache_Reject_Download_Type_OPSP = graphPanel.new(
  title='Remote Cache Reject Download Type OPS',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_remote_cache_reject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (reason, file_type, $additional_groupby)',
    legendFormat='{{reason}}-{{file_type}} {{$additional_groupby}}',
  )
)
.addSeriesOverride({ alias: '', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
  decimals=0,
)
.addYaxis(
  format='s',
);

local remote_Cache_UsageP = graphPanel.new(
  title='Remote Cache Usage',
  datasource=common.datasource,
  description='Remote Cache Usage',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_hideZero=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_DTFileCacheCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='DTFileCapacity-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_DTFileCacheUsed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='DTFileUsed-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_PageCacheCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='PageCapacity-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_PageCacheUsed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='PageUsed-{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
  show=false,
);

local memory_Usage_of_Storage_TasksP = graphPanel.new(
  title='Memory Usage of Storage Tasks',
  datasource=common.datasource,
  description='Memory Usage of Storage Tasks',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_max=true,
  legend_hideZero=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_MemoryTrackingQueryStorageTask{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='MemoryTrackingQueryStorageTask-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_MemoryTrackingFetchPages{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='MemoryTrackingFetchPages-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_DT_DeltaIndexCacheSize{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='DeltaIndexCacheSize-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_MemoryTrackingSharedColumnData{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='SharedColumnData-{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
  show=false,
);

local mVCCIndexCacheP = graphPanel.new(
  title='MVCCIndexCache',
  datasource=common.datasource,
  description='DeltaIndex cache of ReadNodes',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_avg=true,
  legend_hideZero=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_mvcc_index_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, instance)',
    legendFormat='{{type}}-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_mvcc_index_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"hit"}[1m])) by (instance) /sum(rate(tiflash_storage_mvcc_index_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='hit_ratio-{{instance}}',
  )
)
.addSeriesOverride({ alias: '/hit_ratio/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
);

local placeIndex_Tasks_DurationP = graphPanel.new(
  title='PlaceIndex Tasks Duration',
  datasource=common.datasource,
  description='Duration of storage\'s internal sub tasks',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval]))) by (le,type, $additional_groupby) / 1000000000)',
    legendFormat='max-{{type}} {{$additional_groupby}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9999, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval])) by (le, type, $additional_groupby))',
    legendFormat='9999-{{type}} {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval])) by (le, type, $additional_groupby))',
    legendFormat='99-{{type}} {{$additional_groupby}}',
  )
)
.resetYaxes()
.addYaxis(
  format='s',
  min='0',
  decimals=1,
)
.addYaxis(
  format='s',
  min='0',
  show=false,
);

local placeIndexTask_Reuse_OPSP = graphPanel.new(
  title='PlaceIndexTask/Reuse OPS',
  datasource=common.datasource,
  description='Total number of storage\'s internal sub tasks',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_place_index_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_subtask_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"place_index_update"}[$__rate_interval])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
  )
)
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
  decimals=1,
)
.addYaxis(
  format='opm',
  min='0',
  show=false,
);

local placeIndex_update_rows_deletesP = graphPanel.new(
  title='PlaceIndex update rows/deletes',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_place_index_stats_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, $additional_groupby) / 1000000000)',
    legendFormat='max {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_place_index_stats_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, $additional_groupby))',
    legendFormat='99-{{type}} {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum by (type, $additional_groupby) (rate(tiflash_storage_place_index_stats_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) / sum by (type, $additional_groupby) (rate(tiflash_storage_place_index_stats_count_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='avg-{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
  decimals=2,
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([read_Duration_BreakdownP]),
      common.band([remote_Cache_OperationsP, remote_Cache_FlowP]),
      common.band([remote_Cache_BG_Download_DurationP, remote_Cache_Wait_on_Downloading_DurationP]),
      common.band([remote_Cache_Wait_on_Downloading_OPSP, remote_Cache_Wait_on_Downloading_FlowP]),
      common.band([remote_Cache_GaugeP, remote_Cache_Reject_Download_Type_OPSP]),
      common.band([remote_Cache_UsageP, memory_Usage_of_Storage_TasksP]),
      common.band([mVCCIndexCacheP, placeIndex_Tasks_DurationP]),
      common.band([placeIndexTask_Reuse_OPSP, placeIndex_update_rows_deletesP])
    ],
  ),
}
