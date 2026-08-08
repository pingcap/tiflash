// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Imbalance read/write');

local cPU_Usage_irateP = graphPanel.new(
  title='CPU Usage (irate)',
  datasource=common.datasource,
  description='TiFlash CPU usage calculated with process CPU running seconds.',
  formatY1='percentunit',
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
    'irate(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$tiflash_role"}[1m])',
    legendFormat='{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='limit-{{instance}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 });

local segment_ReaderP = graphPanel.new(
  title='Segment Reader',
  datasource=common.datasource,
  formatY1='percentunit',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"SegmentReader.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"SegmentReader.*", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local request_QPS_by_instanceP = graphPanel.new(
  title='Request QPS by instance',
  datasource=common.datasource,
  formatY1='none',
  formatY2='none',
  min='0',
  fill=0,
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
    'sum(rate(tiflash_coprocessor_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, instance)',
    legendFormat='{{type}}-{{instance}}',
    intervalFactor=1,
  )
);

local read_Throughput_by_instanceP = graphPanel.new(
  title='Read Throughput by instance',
  datasource=common.datasource,
  description='The flow of different kinds of read operations',
  formatY1='binBps',
  formatY2='short',
  min='0',
  fill=1,
  nullPointMode='null',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='File Descriptor-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_PSMReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='Page-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='PageBackGround-{{instance}}',
    intervalFactor=1,
  )
);

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


{
  row: rowObj
  .addPanel(cPU_Usage_irateP, gridPos=common.pos(12, 8))
  .addPanel(segment_ReaderP, gridPos=common.pos(12, 8))
  .addPanel(request_QPS_by_instanceP, gridPos=common.pos(12, 8))
  .addPanel(read_Throughput_by_instanceP, gridPos=common.pos(12, 8))
  .addPanel(write_Command_OPS_By_InstanceP, gridPos=common.pos(12, 8))
  .addPanel(write_Throughput_By_InstanceP, gridPos=common.pos(12, 8))
  ,
  panels: [
    { panel: cPU_Usage_irateP, w: 12, h: 8 },
    { panel: segment_ReaderP, w: 12, h: 8 },
    { panel: request_QPS_by_instanceP, w: 12, h: 8 },
    { panel: read_Throughput_by_instanceP, w: 12, h: 8 },
    { panel: write_Command_OPS_By_InstanceP, w: 12, h: 8 },
    { panel: write_Throughput_By_InstanceP, w: 12, h: 8 }
  ],
}
