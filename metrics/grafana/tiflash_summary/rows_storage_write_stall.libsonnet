// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Storage Write Stall');

local write_Stall_DurationP = graphPanel.new(
  title='Write Stall Duration',
  datasource=common.datasource,
  description='The stall duration of write and delete range',
  formatY1='s',
  formatY2='s',
  min='0',
  fill=0,
  nullPointMode='null as zero',
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
    'histogram_quantile(0.99, sum(rate(tiflash_storage_write_stall_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, instance))',
    legendFormat='99-{{type}}-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_write_stall_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type, instance) / 1000000000)',
    legendFormat='max-{{type}}-{{instance}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '99-delta_merge', yaxis: 2 });

local write_Delta_Management_ThroughputP = graphPanel.new(
  title='Write & Delta Management Throughput',
  datasource=common.datasource,
  description='The throughput of write and delta\'s background management',
  formatY1='binBps',
  formatY2='bytes',
  min='0',
  fill=0,
  nullPointMode='null',
  decimals=1,
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
    'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[1m]))',
    legendFormat='write+ingest',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"write|ingest"}[1m]))',
    legendFormat='ManageDelta',
    intervalFactor=1,
  )
);

local write_Delta_Management_TotalP = graphPanel.new(
  title='Write & Delta Management Total',
  datasource=common.datasource,
  description='The throughput of write and delta\'s background management',
  formatY1='bytes',
  formatY2='bytes',
  min='0',
  fill=0,
  nullPointMode='null',
  decimals=1,
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
    'sum(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"})',
    legendFormat='write+ingest',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"write|ingest"})',
    legendFormat='ManageDelta',
    intervalFactor=1,
  )
);

local write_Throughput_By_InstanceP = graphPanel.new(
  title='Write Throughput By Instance',
  datasource=common.datasource,
  description='The throughput of write by instance',
  formatY1='binBps',
  formatY2='bytes',
  min='0',
  fill=0,
  nullPointMode='null',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
  legend_sideWidth=250,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write"}[1m])) by (instance)',
    legendFormat='write-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ingest"}[1m])) by (instance)',
    legendFormat='ingest-{{instance}}',
  )
)
.addSeriesOverride({ alias: '/total/', yaxis: 2 });

local write_Command_OPS_By_InstanceP = graphPanel.new(
  title='Write Command OPS By Instance',
  datasource=common.datasource,
  description='The total count of different kinds of commands received',
  formatY1='ops',
  formatY2='opm',
  min='0',
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
    'sum(rate(tiflash_system_profile_event_DMWriteBlock{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
    legendFormat='write block-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
    legendFormat='{{type}}-{{instance}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/delete_range|ingest/', yaxis: 2 });


{
  row: rowObj
  .addPanel(write_Stall_DurationP, gridPos=common.pos(24, 8, x=0, y=61))
  .addPanel(write_Delta_Management_ThroughputP, gridPos=common.pos(12, 8, x=0, y=69))
  .addPanel(write_Delta_Management_TotalP, gridPos=common.pos(12, 8, x=12, y=69))
  .addPanel(write_Throughput_By_InstanceP, gridPos=common.pos(24, 9, x=0, y=77))
  .addPanel(write_Command_OPS_By_InstanceP, gridPos=common.pos(24, 9, x=0, y=86))
  ,
  panels: [
    { panel: write_Stall_DurationP, w: 24, h: 8, x: 0, y: 61 },
    { panel: write_Delta_Management_ThroughputP, w: 12, h: 8, x: 0, y: 69 },
    { panel: write_Delta_Management_TotalP, w: 12, h: 8, x: 12, y: 69 },
    { panel: write_Throughput_By_InstanceP, w: 24, h: 9, x: 0, y: 77 },
    { panel: write_Command_OPS_By_InstanceP, w: 24, h: 9, x: 0, y: 86 }
  ],
}
