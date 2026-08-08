// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Storage Read Pool & Data Sharing');

local panelReadTasksOps = common.opsPanel(
  'Read Tasks OPS',
  'tiflash_storage_read_tasks_count',
  by=['instance'],
  description='Total number of storage engine read tasks',
);

local panelReadSnapshots = graphPanel.new(
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

local panelReadThreadInternalDuration = common.durationPanel(
  'Read Thread Internal Duration',
  'tiflash_read_thread_internal_us_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
  unit='µs',
);

local panelReadThreadScheduling = graphPanel.new(
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

local panelDataSharing = common.opsHitRatioPanel(
  'Data Sharing',
  'tiflash_storage_read_thread_counter',
  [
    {
      metric: 'tiflash_storage_column_cache_packs',
      hitLabels: 'type=~"data_sharing_hit"',
      totalLabels: 'type=~"data_sharing_hit|data_sharing_miss"',
      legend: 'data_sharing_cache_hit_ratio',
      overrideAlias: '/cache_hit_ratio/',
    },
    {
      metric: 'tiflash_storage_column_cache_packs',
      hitLabels: 'type=~"extra_column_hit"',
      totalLabels: 'type=~"extra_column_hit|extra_column_miss"',
      legend: 'extra_column_cache_hit_ratio',
      overrideAlias: '/cache_hit_ratio/',
      hide: true,
    },
  ],
  by=['type'],
  labels='type=~"add_cache_total_bytes_limit"',
  legend='{{type}}',
  description='The information of data sharing cache hit ratio. Data sharing cache is purpose-built for OLAP workload that can reduce repeated data reads of concurrent table scanning.',
);

local panelSegmentMergedtask = graphPanel.new(
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

local panelSegmentMergedtaskDuration = common.durationPanel(
  'Segment MergedTask Duration',
  'tiflash_storage_read_thread_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
);

local panelVersionChain = common.durationPanel(
  'VersionChain',
  'tiflash_storage_version_chain_ms_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
  unit='ms',
);

local panelDeltaIndexError = graphPanel.new(
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
      common.band([panelReadTasksOps, panelReadSnapshots]),
      common.band([panelReadThreadInternalDuration, panelReadThreadScheduling]),
      common.band([panelDataSharing, panelSegmentMergedtask, panelSegmentMergedtaskDuration]),
      common.band([panelVersionChain, panelDeltaIndexError])
    ],
  ),
}
