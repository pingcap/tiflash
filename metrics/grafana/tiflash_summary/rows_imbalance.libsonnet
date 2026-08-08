// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Imbalance read/write');

local panelCpuUsageIrate = graphPanel.new(
  title='CPU Usage (irate)',
  datasource=common.datasource,
  description='TiFlash CPU usage calculated with process CPU running seconds.',
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
.addSeriesOverride({ alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 })
.resetYaxes()
.addYaxis(
  format='percentunit',
  min='0',
  decimals=1,
)
.addYaxis(
  format='short',
  show=false,
);

local panelSegmentReader = common.cpuWithLimitPanel(
  'Segment Reader',
  'SegmentReader.*',
  legend='{{name}} {{instance}}',
);

local panelRequestQpsByInstance = common.opsPanel(
  'Request QPS by instance',
  'tiflash_coprocessor_request_count',
  by=['type', 'instance'],
  legend='{{type}}-{{instance}}',
  yLeft='none',
);

local panelReadThroughputByInstance = graphPanel.new(
  title='Read Throughput by instance',
  datasource=common.datasource,
  description='The flow of different kinds of read operations',
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
)
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
)
.addYaxis(
  format='short',
  min='0',
);

local panelWriteCommandOpsByInstance = graphPanel.new(
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

local panelWriteThroughputByInstance = graphPanel.new(
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


{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelCpuUsageIrate, panelSegmentReader]),
      common.band([panelRequestQpsByInstance, panelReadThroughputByInstance]),
      common.band([panelWriteCommandOpsByInstance, panelWriteThroughputByInstance])
    ],
  ),
}
