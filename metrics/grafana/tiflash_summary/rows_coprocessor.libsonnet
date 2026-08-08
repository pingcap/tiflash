// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Coprocessor');

local request_QPSP = graphPanel.new(
  title='Request QPS',
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
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_coprocessor_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
);

local executor_QPSP = graphPanel.new(
  title='Executor QPS',
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
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_coprocessor_executor_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
);

local request_DurationP = graphPanel.new(
  title='Request Duration',
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
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='80-{{type}}',
    intervalFactor=1,
    hide=true,
  )
);

local error_QPSP = graphPanel.new(
  title='Error QPS',
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
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_coprocessor_request_error{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (reason)',
    legendFormat='{{reason}}',
    intervalFactor=1,
  )
);

local request_Handle_DurationP = graphPanel.new(
  title='Request Handle Duration',
  datasource=common.datasource,
  formatY1='s',
  formatY2='short',
  min='0',
  fill=1,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='80-{{type}}',
    intervalFactor=1,
  )
);

local response_Bytes_SecondsP = graphPanel.new(
  title='Response Bytes/Seconds',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
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
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_coprocessor_response_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
);

local cop_task_memory_usageP = graphPanel.new(
  title='Cop task memory usage',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
  min='0',
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
);

local exchange_Bytes_SecondsP = graphPanel.new(
  title='Exchange Bytes/Seconds',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
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
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_exchange_data_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
);

local threads_of_RpcP = graphPanel.new(
  title='Threads of Rpc',
  datasource=common.datasource,
  formatY1='none',
  formatY2='short',
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
    'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"rpc.*", type!~".*max"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
    intervalFactor=1,
  )
);

local handling_Request_NumberP = graphPanel.new(
  title='Handling Request Number',
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
)
.addTarget(
  prometheus.target(
    'sum(tiflash_coprocessor_handling_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
);

local threadsP = graphPanel.new(
  title='Threads',
  datasource=common.datasource,
  formatY1='none',
  formatY2='short',
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
    'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~".*max", type!~"rpc.*"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
    intervalFactor=1,
  )
);

local max_Threads_of_RpcP = graphPanel.new(
  title='Max Threads of Rpc',
  datasource=common.datasource,
  formatY1='none',
  formatY2='short',
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
    'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"rpc.*", type=~".*max"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
    intervalFactor=1,
  )
);

local mPP_Query_countP = graphPanel.new(
  title='MPP Query count',
  datasource=common.datasource,
  description='The MPP query count in TiFlash',
  formatY1='none',
  formatY2='short',
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
    'max(tiflash_mpp_task_manager{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
    intervalFactor=1,
  )
);

local max_ThreadsP = graphPanel.new(
  title='Max Threads',
  datasource=common.datasource,
  formatY1='none',
  formatY2='short',
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
    'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*max", type!~"rpc.*"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
    intervalFactor=1,
  )
);

local time_of_the_Longest_Live_MPP_TaskP = graphPanel.new(
  title='Time of the Longest Live MPP Task',
  datasource=common.datasource,
  formatY1='s',
  formatY2='short',
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
    'tiflash_mpp_task_monitor{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='{{instance}}',
    intervalFactor=1,
  )
);

local data_size_in_send_and_receive_queueP = graphPanel.new(
  title='Data size in send and receive queue',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
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
)
.addTarget(
  prometheus.target(
    'tiflash_exchange_queueing_data_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
);

local network_TransmissionP = graphPanel.new(
  title='Network Transmission',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
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
    'sum(rate(tiflash_network_transmission_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
);

local establish_calldata_detailsP = graphPanel.new(
  title='Establish calldata details',
  datasource=common.datasource,
  description='The establish calldata details',
  formatY1='none',
  formatY2='short',
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
)
.addTarget(
  prometheus.target(
    'max(tiflash_establish_calldata_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type != "new_request_calldata"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
    intervalFactor=1,
  )
);


{
  row: rowObj
  .addPanel(request_QPSP, gridPos=common.pos(12, 7, x=0, y=36))
  .addPanel(executor_QPSP, gridPos=common.pos(12, 7, x=12, y=36))
  .addPanel(request_DurationP, gridPos=common.pos(12, 7, x=0, y=43))
  .addPanel(error_QPSP, gridPos=common.pos(12, 7, x=12, y=43))
  .addPanel(request_Handle_DurationP, gridPos=common.pos(12, 7, x=0, y=50))
  .addPanel(response_Bytes_SecondsP, gridPos=common.pos(12, 7, x=12, y=50))
  .addPanel(cop_task_memory_usageP, gridPos=common.pos(12, 7, x=0, y=57))
  .addPanel(exchange_Bytes_SecondsP, gridPos=common.pos(12, 7, x=12, y=57))
  .addPanel(threads_of_RpcP, gridPos=common.pos(12, 7, x=0, y=64))
  .addPanel(handling_Request_NumberP, gridPos=common.pos(12, 7, x=12, y=64))
  .addPanel(threadsP, gridPos=common.pos(12, 7, x=0, y=71))
  .addPanel(max_Threads_of_RpcP, gridPos=common.pos(12, 7, x=12, y=71))
  .addPanel(mPP_Query_countP, gridPos=common.pos(12, 7, x=0, y=78))
  .addPanel(max_ThreadsP, gridPos=common.pos(12, 7, x=12, y=78))
  .addPanel(time_of_the_Longest_Live_MPP_TaskP, gridPos=common.pos(12, 7, x=0, y=85))
  .addPanel(data_size_in_send_and_receive_queueP, gridPos=common.pos(12, 7, x=12, y=85))
  .addPanel(network_TransmissionP, gridPos=common.pos(12, 7, x=0, y=92))
  .addPanel(establish_calldata_detailsP, gridPos=common.pos(12, 7, x=12, y=92))
  ,
  panels: [
    { panel: request_QPSP, w: 12, h: 7, x: 0, y: 36 },
    { panel: executor_QPSP, w: 12, h: 7, x: 12, y: 36 },
    { panel: request_DurationP, w: 12, h: 7, x: 0, y: 43 },
    { panel: error_QPSP, w: 12, h: 7, x: 12, y: 43 },
    { panel: request_Handle_DurationP, w: 12, h: 7, x: 0, y: 50 },
    { panel: response_Bytes_SecondsP, w: 12, h: 7, x: 12, y: 50 },
    { panel: cop_task_memory_usageP, w: 12, h: 7, x: 0, y: 57 },
    { panel: exchange_Bytes_SecondsP, w: 12, h: 7, x: 12, y: 57 },
    { panel: threads_of_RpcP, w: 12, h: 7, x: 0, y: 64 },
    { panel: handling_Request_NumberP, w: 12, h: 7, x: 12, y: 64 },
    { panel: threadsP, w: 12, h: 7, x: 0, y: 71 },
    { panel: max_Threads_of_RpcP, w: 12, h: 7, x: 12, y: 71 },
    { panel: mPP_Query_countP, w: 12, h: 7, x: 0, y: 78 },
    { panel: max_ThreadsP, w: 12, h: 7, x: 12, y: 78 },
    { panel: time_of_the_Longest_Live_MPP_TaskP, w: 12, h: 7, x: 0, y: 85 },
    { panel: data_size_in_send_and_receive_queueP, w: 12, h: 7, x: 12, y: 85 },
    { panel: network_TransmissionP, w: 12, h: 7, x: 0, y: 92 },
    { panel: establish_calldata_detailsP, w: 12, h: 7, x: 12, y: 92 }
  ],
}
