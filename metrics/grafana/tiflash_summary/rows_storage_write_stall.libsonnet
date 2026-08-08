// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Storage Write Stall');

local write_Stall_DurationP = common.durationPanel(
  'Write Stall Duration',
  'tiflash_storage_write_stall_duration_seconds_bucket',
  by=['type', 'instance'],
  legend='%s-{{type}}-{{instance}}',
  description='The stall duration of write and delete range',
)
.addSeriesOverride({ alias: '99-delta_merge', yaxis: 2 });

local write_Delta_Management_ThroughputP = graphPanel.new(
  title='Write & Delta Management Throughput',
  datasource=common.datasource,
  description='The throughput of write and delta\'s background management',
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
)
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
)
.addYaxis(
  format='bytes',
  show=false,
);

local write_Delta_Management_TotalP = graphPanel.new(
  title='Write & Delta Management Total',
  datasource=common.datasource,
  description='The throughput of write and delta\'s background management',
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
)
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='bytes',
  show=false,
);

local write_Throughput_By_InstanceP = graphPanel.new(
  title='Write Throughput By Instance',
  datasource=common.datasource,
  description='The throughput of write by instance',
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
.addSeriesOverride({ alias: '/total/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
)
.addYaxis(
  format='bytes',
  show=false,
);

local write_Command_OPS_By_InstanceP = graphPanel.new(
  title='Write Command OPS By Instance',
  datasource=common.datasource,
  description='The total count of different kinds of commands received',
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
.addSeriesOverride({ alias: '/delete_range|ingest/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([write_Stall_DurationP]),
      common.band([write_Delta_Management_ThroughputP, write_Delta_Management_TotalP]),
      common.band([write_Throughput_By_InstanceP]),
      common.band([write_Command_OPS_By_InstanceP])
    ],
  ),
}
