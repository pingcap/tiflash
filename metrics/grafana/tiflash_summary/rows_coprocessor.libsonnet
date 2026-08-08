// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Coprocessor');

local panelRequestQps = common.opsPanel(
  'Request QPS',
  'tiflash_coprocessor_request_count',
  by=['type'],
  yLeft='none',
);

local panelExecutorQps = common.opsPanel(
  'Executor QPS',
  'tiflash_coprocessor_executor_count',
  by=['type'],
  yLeft='none',
);

local panelRequestDuration = common.durationPanel(
  'Request Duration',
  'tiflash_coprocessor_request_duration_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
);

local panelErrorQps = common.opsPanel(
  'Error QPS',
  'tiflash_coprocessor_request_error',
  by=['reason'],
  legend='{{reason}}',
  yLeft='none',
);

local panelRequestHandleDuration = common.durationPanel(
  'Request Handle Duration',
  'tiflash_coprocessor_request_handle_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
);

local panelResponseBytesSeconds = graphPanel.new(
  title='Response Bytes/Seconds',
  datasource=common.datasource,
  fill=0,
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
    'sum(rate(tiflash_coprocessor_response_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='short',
);

local panelCopTaskMemoryUsage = graphPanel.new(
  title='Cop task memory usage',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  pointradius=2,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_coprocessor_request_memory_usage_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_coprocessor_request_memory_usage_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_coprocessor_request_memory_usage_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tiflash_coprocessor_request_memory_usage_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='80-{{type}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='short',
);

local panelExchangeBytesSeconds = graphPanel.new(
  title='Exchange Bytes/Seconds',
  datasource=common.datasource,
  fill=0,
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
    'sum(rate(tiflash_exchange_data_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='short',
);

local panelThreadsOfRpc = graphPanel.new(
  title='Threads of Rpc',
  datasource=common.datasource,
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
    'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"rpc.*", type!~".*max"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
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

local panelHandlingRequestNumber = graphPanel.new(
  title='Handling Request Number',
  datasource=common.datasource,
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
    'sum(tiflash_coprocessor_handling_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  min='0',
)
.addYaxis(
  format='none',
);

local panelThreads = graphPanel.new(
  title='Threads',
  datasource=common.datasource,
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
    'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~".*max", type!~"rpc.*"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
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

local panelMaxThreadsOfRpc = graphPanel.new(
  title='Max Threads of Rpc',
  datasource=common.datasource,
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
    'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"rpc.*", type=~".*max"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
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

local panelMppQueryCount = graphPanel.new(
  title='MPP Query count',
  datasource=common.datasource,
  description='The MPP query count in TiFlash',
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
    'max(tiflash_mpp_task_manager{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
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

local panelMaxThreads = graphPanel.new(
  title='Max Threads',
  datasource=common.datasource,
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
    'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*max", type!~"rpc.*"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
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

local panelTimeOfTheLongestLiveMppTask = graphPanel.new(
  title='Time of the Longest Live MPP Task',
  datasource=common.datasource,
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
    'tiflash_mpp_task_monitor{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='{{instance}}',
    intervalFactor=1,
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

local panelDataSizeInSendAndReceiveQueue = graphPanel.new(
  title='Data size in send and receive queue',
  datasource=common.datasource,
  fill=0,
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
    'tiflash_exchange_queueing_data_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='short',
);

local panelNetworkTransmission = graphPanel.new(
  title='Network Transmission',
  datasource=common.datasource,
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
    'sum(rate(tiflash_network_transmission_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='short',
);

local panelEstablishCalldataDetails = graphPanel.new(
  title='Establish calldata details',
  datasource=common.datasource,
  description='The establish calldata details',
  fill=0,
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
    'max(tiflash_establish_calldata_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type != "new_request_calldata"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
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


{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelRequestQps, panelExecutorQps]),
      common.band([panelRequestDuration, panelErrorQps]),
      common.band([panelRequestHandleDuration, panelResponseBytesSeconds]),
      common.band([panelCopTaskMemoryUsage, panelExchangeBytesSeconds]),
      common.band([panelThreadsOfRpc, panelHandlingRequestNumber]),
      common.band([panelThreads, panelMaxThreadsOfRpc]),
      common.band([panelMppQueryCount, panelMaxThreads]),
      common.band([panelTimeOfTheLongestLiveMppTask, panelDataSizeInSendAndReceiveQueue]),
      common.band([panelNetworkTransmission, panelEstablishCalldataDetails])
    ],
  ),
}
