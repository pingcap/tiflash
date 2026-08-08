// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Raft');

local stale_Read_OPSP = graphPanel.new(
  title='Stale Read OPS',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_stale_read_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='{{instance}}',
    intervalFactor=1,
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

local raft_Read_Index_OPSP = graphPanel.new(
  title='Raft Read Index OPS',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_read_index_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='{{instance}}',
    intervalFactor=1,
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

local learner_Read_FailuresP = graphPanel.new(
  title='Learner Read Failures',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_learner_read_failures_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
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

local read_Index_EventsP = graphPanel.new(
  title='Read Index Events',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_read_index_events_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
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

local raft_Wait_Index_DurationP = graphPanel.new(
  title='Raft Wait Index Duration',
  datasource=common.datasource,
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_raft_wait_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, $additional_groupby) / 1000000000)',
    legendFormat='max {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='99 {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='95 {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='80 {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(increase(tiflash_system_profile_event_RaftWaitIndexTimeout{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='{{instance}}-timeout',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/timeout/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='s',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
  decimals=2,
);

local raft_Batch_Read_Index_DurationP = common.durationPanel(
  'Raft Batch Read Index Duration',
  'tiflash_raft_read_index_duration_seconds_bucket',
  description='The number of currently applying snapshots.',
);

local apply_Raft_write_logs_DurationP = graphPanel.new(
  title='Apply Raft write logs Duration',
  datasource=common.datasource,
  description='Duration of applying Raft write logs',
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type) / 1000000000)',
    legendFormat=' 100%-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat=' 99%-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_apply_write_command_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="write"}[1m])) / sum(rate(tiflash_raft_apply_write_command_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="write"}[1m]))',
    legendFormat='avg-write',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_apply_write_command_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="admin"}[1m])) / sum(rate(tiflash_raft_apply_write_command_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="admin"}[1m]))',
    legendFormat='avg-admin',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_apply_write_command_duration_seconds_sum{k8s_cluster="$k8s_cluster", cluster_id=~".*$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="flush_region"}[1m])) / sum(rate(tiflash_raft_apply_write_command_duration_seconds_count{k8s_cluster="$k8s_cluster", cluster_id=~".*$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="flush_region"}[1m]))',
    legendFormat='avg-flush_region',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_write_data_to_storage_duration_seconds_sum{k8s_cluster="$k8s_cluster", cluster_id=~".*$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="decode"}[1m])) / sum(rate(tiflash_raft_write_data_to_storage_duration_seconds_count{k8s_cluster="$k8s_cluster", cluster_id=~".*$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="decode"}[1m]) )',
    legendFormat='avg-decode',
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

local region_write_Duration_decodeP = heatmapPanel.new(
  title='Region write Duration (decode)',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='s',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  description='Duration of decoding Region data into blocks when writing Region data to the storage layer. (Mixed with "write logs" and "apply Snapshot" operations)',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_write_data_to_storage_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="decode"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local region_write_Duration_write_blocksP = heatmapPanel.new(
  title='Region write Duration (write blocks)',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='s',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  description='Duration of writing Region data blocks to the storage layer (Mixed with "write logs" and "apply Snapshot" operations)',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_write_data_to_storage_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="write"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local apply_Raft_write_logs_Duration_HeatmapP = heatmapPanel.new(
  title='Apply Raft write logs Duration [Heatmap]',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='s',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  description='Duration of applying Raft write logs',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="write"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local apply_Raft_admin_logs_Duration_HeatmapP = heatmapPanel.new(
  title='Apply Raft admin logs Duration [Heatmap]',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='s',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  description='Duration of applying Raft write logs',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="admin"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local raft_Events_QPSP = graphPanel.new(
  title='Raft Events QPS',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_raft_events_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
    legendFormat='{{instance}}',
    intervalFactor=1,
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

local raft_Frequent_Events_QPSP = graphPanel.new(
  title='Raft Frequent Events QPS',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_raft_frequent_events_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
    legendFormat='{{instance}}',
    intervalFactor=1,
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

local raft_Log_Gap_HeatmapP = heatmapPanel.new(
  title='Raft Log Gap Heatmap',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='none',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_raft_log_gap_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"applied_index"}[1m])) by (le, type)',
    format='heatmap',
    legendFormat='{{le}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_raft_log_gap_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"compact_index"}[1m])) by (le, type)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local raft_Entry_Batch_Size_HeatmapP = heatmapPanel.new(
  title='Raft Entry Batch Size Heatmap',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='none',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_entry_size_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"normal"}[1m])) by (le, type)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local region_Size_by_event_HeatmapP = heatmapPanel.new(
  title='Region Size (by event) Heatmap',
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
    'sum(delta(tiflash_raft_region_flush_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"unflushed"}[1m])) by (le, type)',
    format='heatmap',
    legendFormat='{{le}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_region_flush_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"flushed"}[1m])) by (le, type)',
    format='heatmap',
    legendFormat='{{le}}',
    hide=true,
  )
);

local big_Write_To_Region_Size_HeatmapP = heatmapPanel.new(
  title='Big Write To Region Size Heatmap',
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
    'sum(delta(tiflash_raft_write_flow_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"big_write_to_region"}[1m])) by (le, type)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local write_Committed_Size_HeatmapP = heatmapPanel.new(
  title='Write Committed Size Heatmap',
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
    'sum(delta(tiflash_raft_write_flow_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write_committed"}[1m])) by (le, type)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local raft_Eager_GC_OPSP = graphPanel.new(
  title='Raft Eager GC OPS',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_eager_gc_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
    legendFormat='{{instance}}',
    intervalFactor=1,
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

local raft_Eager_GC_DurationP = common.durationPanel(
  'Raft Eager GC Duration',
  'tiflash_raft_eager_gc_duration_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
  description='Duration of Raft logs eager GC tasks',
);

local keys_flowP = graphPanel.new(
  title='Keys flow',
  datasource=common.datasource,
  description='The keys flow of different kinds of Raft operations',
  fill=1,
  nullPointMode='null',
  decimals=1,
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
    'sum(rate(tiflash_raft_process_keys{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
)
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
)
.addYaxis(
  format='short',
  min='0',
);

local raft_throughputP = graphPanel.new(
  title='Raft throughput',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null',
  decimals=1,
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
    'sum(rate(tiflash_raft_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
)
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
)
.addYaxis(
  format='short',
  min='0',
);

local upstream_Latency_HeatmapP = heatmapPanel.new(
  title='Upstream Latency [Heatmap]',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='s',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  description='Latency that TiKV sends raft log to TiFlash.',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local upstream_LatencyP = graphPanel.new(
  title='Upstream Latency',
  datasource=common.datasource,
  description='Latency that TiKV sends raft log to TiFlash.',
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le) / 1000000000)',
    legendFormat=' 100%',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le))',
    legendFormat=' 99%',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le))',
    legendFormat='95%',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_raft_upstream_latency_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) / sum(rate(tiflash_raft_upstream_latency_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='avg',
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

local log_Replication_RejectedP = graphPanel.new(
  title='Log Replication Rejected',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_proxy_tikv_server_raft_append_rejects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='{{instance}}',
    intervalFactor=1,
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


{
  row: common.buildRow(
    rowObj,
    [
      common.band([stale_Read_OPSP, raft_Read_Index_OPSP]),
      common.band([learner_Read_FailuresP, read_Index_EventsP]),
      common.band([raft_Wait_Index_DurationP, raft_Batch_Read_Index_DurationP]),
      common.band([apply_Raft_write_logs_DurationP]),
      common.band([region_write_Duration_decodeP, region_write_Duration_write_blocksP]),
      common.band([apply_Raft_write_logs_Duration_HeatmapP, apply_Raft_admin_logs_Duration_HeatmapP]),
      common.band([raft_Events_QPSP, raft_Frequent_Events_QPSP]),
      common.band([raft_Log_Gap_HeatmapP, raft_Entry_Batch_Size_HeatmapP]),
      common.band([region_Size_by_event_HeatmapP, big_Write_To_Region_Size_HeatmapP]),
      common.band([write_Committed_Size_HeatmapP]),
      common.band([raft_Eager_GC_OPSP, raft_Eager_GC_DurationP]),
      common.band([keys_flowP]),
      common.band([raft_throughputP]),
      common.band([upstream_Latency_HeatmapP, upstream_LatencyP]),
      common.band([{ panel: log_Replication_RejectedP, w: 12 }])
    ],
  ),
}
