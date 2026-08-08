// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Raft Snapshot / IngestSST');

local heavy_Raft_Apply_DurationP = common.durationPanel(
  'Heavy Raft Apply Duration',
  'tiflash_raft_command_duration_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
);

local applying_snapshots_CountP = graphPanel.new(
  title='Applying snapshots Count',
  datasource=common.datasource,
  description='The number of currently applying snapshots.',
  fill=1,
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
    'sum(tiflash_system_current_metric_RaftNumSnapshotsPendingApply{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='Pending-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_RaftNumPrehandlingSubTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='PrehandleSubtasks-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_RaftNumParallelPrehandlingTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='ParallelTasks-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_RaftNumWaitedParallelPrehandlingTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='Pending-ParallelTasks-{{instance}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  min='0',
)
.addYaxis(
  format='short',
);

local snapshot_Uncommitted_Size_HeatmapP = common.heatmap(
  'Snapshot Uncommitted Size Heatmap',
  'tiflash_raft_write_flow_bytes_bucket',
  yFormat='bytes',
  labels='type=~"snapshot_uncommitted"',
  by=['le', 'type'],
);

local ongoing_raft_snapshotP = graphPanel.new(
  title='Ongoing raft snapshot',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null',
  pointradius=2,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_ongoing_snapshot_total_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
    legendFormat='{{le}}',
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
)
.addYaxis(
  format='short',
);

local snapshot_Size_HeatmapP = common.heatmap(
  'Snapshot Size Heatmap',
  'tiflash_raft_snapshot_total_bytes_bucket',
  yFormat='bytes',
  labels='type="approx_raft_snapshot"',
);

local snapshot_Predecode_DurationP = common.heatmap(
  'Snapshot Predecode Duration',
  'tiflash_raft_command_duration_seconds_bucket',
  yFormat='s',
  labels='type="snapshot_predecode"',
  description='Duration of pre-decode when applying region snapshot',
);

local snapshot_Prehandle_Throughput_HeatmapP = common.heatmap(
  'Snapshot Prehandle Throughput Heatmap',
  'tiflash_raft_command_throughput_seconds_bucket',
  yFormat='bytes',
  labels='type="prehandle_snapshot"',
);

local snapshot_Flush_DurationP = common.heatmap(
  'Snapshot Flush Duration',
  'tiflash_raft_command_duration_seconds_bucket',
  yFormat='s',
  labels='type="snapshot_flush"',
  description='Duration of pre-decode when applying region snapshot',
);

local ingest_Uncommitted_Size_HeatmapP = common.heatmap(
  'Ingest Uncommitted Size Heatmap',
  'tiflash_raft_write_flow_bytes_bucket',
  yFormat='bytes',
  labels='type=~"ingest_uncommitted"',
  by=['le', 'type'],
);

local snapshot_Predecode_SST_to_DT_DurationP = common.heatmap(
  'Snapshot Predecode SST to DT Duration',
  'tiflash_raft_command_duration_seconds_bucket',
  yFormat='s',
  labels='type="snapshot_predecode_sst2dt"',
  description='Duration of SST to DT in pre-decode when applying region snapshot',
);

local ingest_SST_DurationP = common.heatmap(
  'Ingest SST Duration',
  'tiflash_raft_command_duration_seconds_bucket',
  yFormat='s',
  labels='type="ingest_sst"',
  description='Duration of ingesting SST',
);

{
  row: common.buildRow(
    rowObj,
    [
      common.band([heavy_Raft_Apply_DurationP]),
      common.band([applying_snapshots_CountP]),
      common.band([snapshot_Uncommitted_Size_HeatmapP, ongoing_raft_snapshotP]),
      common.band([snapshot_Size_HeatmapP, snapshot_Predecode_DurationP]),
      common.band([snapshot_Prehandle_Throughput_HeatmapP, snapshot_Flush_DurationP]),
      common.band([ingest_Uncommitted_Size_HeatmapP, snapshot_Predecode_SST_to_DT_DurationP]),
      common.band([{ panel: ingest_SST_DurationP, w: 12 }])
    ],
  ),
}
