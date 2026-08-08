// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Storage Read Pool & Data Sharing');

local read_Tasks_OPSP = graphPanel.new(
  title='Read Tasks OPS',
  datasource=common.datasource,
  description='Total number of storage engine read tasks',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_avg=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_read_tasks_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='none',
);

local read_SnapshotsP = graphPanel.new(
  title='Read Snapshots',
  datasource=common.datasource,
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
    'tiflash_system_current_metric_PSMVCCSnapshotsList{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='snapshot_list-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_PSMVCCNumSnapshots{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    format='heatmap',
    legendFormat='num_snapshot-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_SnapshotOfRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='read-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_SnapshotOfReadRaw{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='read_raw-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_SnapshotOfDeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='delta_merge-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_SnapshotOfDeltaCompact{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='delta_compact-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_SnapshotOfSegmentMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='seg_merge-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_SnapshotOfSegmentSplit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='seg_split-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_current_metric_DT_SnapshotOfPlaceIndex{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='place_index-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_asynchronous_metric_MaxDTDeltaOldestSnapshotLifetime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='max_snapshot_lifetime-{{instance}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/max_snapshot_lifetime/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
)
.addYaxis(
  format='s',
  min='0',
);

local read_Thread_Internal_DurationP = graphPanel.new(
  title='Read Thread Internal Duration',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_avg=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_read_thread_internal_us_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_read_thread_internal_us_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_read_thread_internal_us_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tiflash_read_thread_internal_us_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='80-{{type}}',
    intervalFactor=1,
    hide=true,
  )
)
.resetYaxes()
.addYaxis(
  format='µs',
  min='0',
)
.addYaxis(
  format='short',
  show=false,
);

local read_Thread_SchedulingP = graphPanel.new(
  title='Read Thread Scheduling',
  datasource=common.datasource,
  description='The information of read thread scheduling.',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_read_thread_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ru_exhausted|sche_active_segment_limit|sche_from_cache|sche_new_task|sche_no_pool|sche_no_ru|sche_no_segment|sche_no_slot|push_block_bytes"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
)
.addSeriesOverride({ alias: '/push_block/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='binBps',
  min='0',
);

local data_SharingP = graphPanel.new(
  title='Data Sharing',
  datasource=common.datasource,
  description='The information of data sharing cache hit ratio. Data sharing cache is purpose-built for OLAP workload that can reduce repeated data reads of concurrent table scanning.',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_read_thread_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"add_cache_total_bytes_limit"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_column_cache_packs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"data_sharing_hit"}[1m]))/sum(rate(tiflash_storage_column_cache_packs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"data_sharing_hit|data_sharing_miss"}[1m]))',
    legendFormat='data_sharing_cache_hit_ratio',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_column_cache_packs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"extra_column_hit"}[1m]))/sum(rate(tiflash_storage_column_cache_packs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"extra_column_hit|extra_column_miss"}[1m]))',
    legendFormat='extra_column_cache_hit_ratio',
    hide=true,
  )
)
.addSeriesOverride({ alias: '/cache_hit_ratio/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
);

local segment_MergedTaskP = graphPanel.new(
  title='Segment MergedTask',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
)
.addTarget(
  prometheus.target(
    'sum by (type,$additional_groupby) (tiflash_storage_read_thread_gauge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
    legendFormat='{{type}} {{$additional_groupby}}',
  )
)
.addSeriesOverride({ alias: '/cache_hit_ratio/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
);

local segment_MergedTask_DurationP = graphPanel.new(
  title='Segment MergedTask Duration',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_avg=true,
  legend_hideZero=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_storage_read_thread_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type,$additional_groupby))',
    legendFormat='999-{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_read_thread_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type,$additional_groupby))',
    legendFormat='99-{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tiflash_storage_read_thread_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type,$additional_groupby))',
    legendFormat='80-{{type}} {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.resetYaxes()
.addYaxis(
  format='s',
  min='0',
)
.addYaxis(
  format='short',
  show=false,
);

local versionChainP = graphPanel.new(
  title='VersionChain',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_avg=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_storage_version_chain_ms_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_version_chain_ms_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_storage_version_chain_ms_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tiflash_storage_version_chain_ms_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='80-{{type}}',
    intervalFactor=1,
    hide=true,
  )
)
.resetYaxes()
.addYaxis(
  format='ms',
  min='0',
)
.addYaxis(
  format='short',
  show=false,
);

local deltaIndexErrorP = graphPanel.new(
  title='DeltaIndexError',
  datasource=common.datasource,
  description='Errors of DeltaIndex',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_DTDeltaIndexError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='DeltaIndexError-{{instance}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='cps',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
  show=false,
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([read_Tasks_OPSP, read_SnapshotsP]),
      common.band([read_Thread_Internal_DurationP, read_Thread_SchedulingP]),
      common.band([data_SharingP, segment_MergedTaskP, segment_MergedTask_DurationP]),
      common.band([versionChainP, deltaIndexErrorP])
    ],
  ),
}
