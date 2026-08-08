// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Raft Snapshot / IngestSST');

local heavy_Raft_Apply_DurationP = graphPanel.new(
  title='Heavy Raft Apply Duration',
  datasource=common.datasource,
  formatY1='s',
  formatY2='short',
  min='0',
  fill=1,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='99%-{{type}}',
  )
);

local applying_snapshots_CountP = graphPanel.new(
  title='Applying snapshots Count',
  datasource=common.datasource,
  description='The number of currently applying snapshots.',
  formatY1='none',
  formatY2='short',
  min='0',
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
);

local snapshot_Uncommitted_Size_HeatmapP = heatmapPanel.new(
  title='Snapshot Uncommitted Size Heatmap',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='bytes',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_write_flow_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"snapshot_uncommitted"}[1m])) by (le, type)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local ongoing_raft_snapshotP = graphPanel.new(
  title='Ongoing raft snapshot',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
  fill=1,
  nullPointMode='null',
  pointradius=2,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_ongoing_snapshot_total_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
    legendFormat='{{le}}',
  )
);

local snapshot_Size_HeatmapP = heatmapPanel.new(
  title='Snapshot Size Heatmap',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='bytes',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_snapshot_total_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="approx_raft_snapshot"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local snapshot_Predecode_DurationP = heatmapPanel.new(
  title='Snapshot Predecode Duration',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='s',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  description='Duration of pre-decode when applying region snapshot',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="snapshot_predecode"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local snapshot_Prehandle_Throughput_HeatmapP = heatmapPanel.new(
  title='Snapshot Prehandle Throughput Heatmap',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='bytes',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_command_throughput_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="prehandle_snapshot"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local snapshot_Flush_DurationP = heatmapPanel.new(
  title='Snapshot Flush Duration',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='s',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  description='Duration of pre-decode when applying region snapshot',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="snapshot_flush"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local ingest_Uncommitted_Size_HeatmapP = heatmapPanel.new(
  title='Ingest Uncommitted Size Heatmap',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='bytes',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_write_flow_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ingest_uncommitted"}[1m])) by (le, type)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local snapshot_Predecode_SST_to_DT_DurationP = heatmapPanel.new(
  title='Snapshot Predecode SST to DT Duration',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='s',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  description='Duration of SST to DT in pre-decode when applying region snapshot',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="snapshot_predecode_sst2dt"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local ingest_SST_DurationP = heatmapPanel.new(
  title='Ingest SST Duration',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='s',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  description='Duration of ingesting SST',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="ingest_sst"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);


{
  row: rowObj
  .addPanel(heavy_Raft_Apply_DurationP, gridPos=common.pos(24, 7))
  .addPanel(applying_snapshots_CountP, gridPos=common.pos(24, 7))
  .addPanel(snapshot_Uncommitted_Size_HeatmapP, gridPos=common.pos(12, 7))
  .addPanel(ongoing_raft_snapshotP, gridPos=common.pos(12, 7))
  .addPanel(snapshot_Size_HeatmapP, gridPos=common.pos(12, 7))
  .addPanel(snapshot_Predecode_DurationP, gridPos=common.pos(12, 7))
  .addPanel(snapshot_Prehandle_Throughput_HeatmapP, gridPos=common.pos(12, 7))
  .addPanel(snapshot_Flush_DurationP, gridPos=common.pos(12, 7))
  .addPanel(ingest_Uncommitted_Size_HeatmapP, gridPos=common.pos(12, 7))
  .addPanel(snapshot_Predecode_SST_to_DT_DurationP, gridPos=common.pos(12, 7))
  .addPanel(ingest_SST_DurationP, gridPos=common.pos(12, 7))
  ,
  panels: [
    { panel: heavy_Raft_Apply_DurationP, w: 24, h: 7 },
    { panel: applying_snapshots_CountP, w: 24, h: 7 },
    { panel: snapshot_Uncommitted_Size_HeatmapP, w: 12, h: 7 },
    { panel: ongoing_raft_snapshotP, w: 12, h: 7 },
    { panel: snapshot_Size_HeatmapP, w: 12, h: 7 },
    { panel: snapshot_Predecode_DurationP, w: 12, h: 7 },
    { panel: snapshot_Prehandle_Throughput_HeatmapP, w: 12, h: 7 },
    { panel: snapshot_Flush_DurationP, w: 12, h: 7 },
    { panel: ingest_Uncommitted_Size_HeatmapP, w: 12, h: 7 },
    { panel: snapshot_Predecode_SST_to_DT_DurationP, w: 12, h: 7 },
    { panel: ingest_SST_DurationP, w: 12, h: 7 }
  ],
}
