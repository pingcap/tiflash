// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Raft');

local panelStaleReadOps = common.opsPanel(
  'Stale Read OPS',
  'tiflash_stale_read_count',
  by=['instance'],
);

local panelRaftReadIndexOps = common.opsPanel(
  'Raft Read Index OPS',
  'tiflash_raft_read_index_count',
  by=['instance'],
);

local panelLearnerReadFailures = common.opsPanel(
  'Learner Read Failures',
  'tiflash_raft_learner_read_failures_count',
  by=['type'],
);

local panelReadIndexEvents = common.opsPanel(
  'Read Index Events',
  'tiflash_raft_read_index_events_count',
  by=['type'],
);

local panelRaftWaitIndexDuration = common.durationPanel(
  'Raft Wait Index Duration',
  'tiflash_raft_wait_index_duration_seconds_bucket',
  yRight='opm',
  extraTargets=[
    common.target(
      common.expr.sumIncrease(
        'tiflash_system_profile_event_RaftWaitIndexTimeout',
        common.selector,
        by=['instance'],
      ),
      '{{instance}}-timeout',
    ),
  ],
  seriesOverrides=[
    common.override('/timeout/', yaxis=2),
  ],
);

local panelRaftBatchReadIndexDuration = common.durationPanel(
  'Raft Batch Read Index Duration',
  'tiflash_raft_read_index_duration_seconds_bucket',
  description='The number of currently applying snapshots.',
);

local panelApplyRaftWriteLogsDuration = common.durationPanel(
  'Apply Raft write logs Duration',
  'tiflash_raft_apply_write_command_duration_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
  description='Duration of applying Raft write logs',
  extraTargets=[
    common.target(
      '(' + common.expr.sumRate('tiflash_raft_apply_write_command_duration_seconds_sum', common.selector, labels='type="write"')
      + ' / ' + common.expr.sumRate('tiflash_raft_apply_write_command_duration_seconds_count', common.selector, labels='type="write"') + ')',
      'avg-write',
    ),
    common.target(
      '(' + common.expr.sumRate('tiflash_raft_apply_write_command_duration_seconds_sum', common.selector, labels='type="admin"')
      + ' / ' + common.expr.sumRate('tiflash_raft_apply_write_command_duration_seconds_count', common.selector, labels='type="admin"') + ')',
      'avg-admin',
    ),
    common.target(
      '(' + common.expr.sumRate('tiflash_raft_apply_write_command_duration_seconds_sum', common.selector, labels='type="flush_region"')
      + ' / ' + common.expr.sumRate('tiflash_raft_apply_write_command_duration_seconds_count', common.selector, labels='type="flush_region"') + ')',
      'avg-flush_region',
    ),
    common.target(
      '(' + common.expr.sumRate('tiflash_raft_write_data_to_storage_duration_seconds_sum', common.selector, labels='type="decode"')
      + ' / ' + common.expr.sumRate('tiflash_raft_write_data_to_storage_duration_seconds_count', common.selector, labels='type="decode"') + ')',
      'avg-decode',
    ),
  ],
);

local panelRegionWriteDurationDecode = common.heatmap(
  'Region write Duration (decode)',
  'tiflash_raft_write_data_to_storage_duration_seconds_bucket',
  yFormat='s',
  labels='type="decode"',
  description='Duration of decoding Region data into blocks when writing Region data to the storage layer. (Mixed with "write logs" and "apply Snapshot" operations)',
);

local panelRegionWriteDurationWriteBlocks = common.heatmap(
  'Region write Duration (write blocks)',
  'tiflash_raft_write_data_to_storage_duration_seconds_bucket',
  yFormat='s',
  labels='type="write"',
  description='Duration of writing Region data blocks to the storage layer (Mixed with "write logs" and "apply Snapshot" operations)',
);

local panelApplyRaftWriteLogsDurationHeatmap = common.heatmap(
  'Apply Raft write logs Duration [Heatmap]',
  'tiflash_raft_apply_write_command_duration_seconds_bucket',
  yFormat='s',
  labels='type="write"',
  description='Duration of applying Raft write logs',
);

local panelApplyRaftAdminLogsDurationHeatmap = common.heatmap(
  'Apply Raft admin logs Duration [Heatmap]',
  'tiflash_raft_apply_write_command_duration_seconds_bucket',
  yFormat='s',
  labels='type="admin"',
  description='Duration of applying Raft write logs',
);

local panelRaftEventsQps = common.opsPanel(
  'Raft Events QPS',
  'tiflash_raft_raft_events_count',
  by=['type'],
);

local panelRaftFrequentEventsQps = common.opsPanel(
  'Raft Frequent Events QPS',
  'tiflash_raft_raft_frequent_events_count',
  by=['type'],
);

local panelRaftLogGapHeatmap = heatmapPanel.new(
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

local panelRaftEntryBatchSizeHeatmap = common.heatmap(
  'Raft Entry Batch Size Heatmap',
  'tiflash_raft_entry_size_bucket',
  yFormat='none',
  labels='type=~"normal"',
  by=['le', 'type'],
);

local panelRegionSizeByEventHeatmap = heatmapPanel.new(
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

local panelBigWriteToRegionSizeHeatmap = common.heatmap(
  'Big Write To Region Size Heatmap',
  'tiflash_raft_write_flow_bytes_bucket',
  yFormat='bytes',
  labels='type=~"big_write_to_region"',
  by=['le', 'type'],
);

local panelWriteCommittedSizeHeatmap = common.heatmap(
  'Write Committed Size Heatmap',
  'tiflash_raft_write_flow_bytes_bucket',
  yFormat='bytes',
  labels='type=~"write_committed"',
  by=['le', 'type'],
);

local panelRaftEagerGcOps = common.opsPanel(
  'Raft Eager GC OPS',
  'tiflash_raft_eager_gc_count',
  by=['type'],
);

local panelRaftEagerGcDuration = common.durationPanel(
  'Raft Eager GC Duration',
  'tiflash_raft_eager_gc_duration_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
  description='Duration of Raft logs eager GC tasks',
);

local panelKeysFlow = graphPanel.new(
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

local panelRaftThroughput = graphPanel.new(
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

local panelUpstreamLatencyHeatmap = common.heatmap(
  'Upstream Latency [Heatmap]',
  'tiflash_raft_upstream_latency_bucket',
  yFormat='s',
  description='Latency that TiKV sends raft log to TiFlash.',
);

local panelUpstreamLatency = common.durationPanel(
  'Upstream Latency',
  'tiflash_raft_upstream_latency_bucket',
  description='Latency that TiKV sends raft log to TiFlash.',
  showAvg=true,
);

local panelLogReplicationRejected = graphPanel.new(
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
      common.band([panelStaleReadOps, panelRaftReadIndexOps]),
      common.band([panelLearnerReadFailures, panelReadIndexEvents]),
      common.band([panelRaftWaitIndexDuration, panelRaftBatchReadIndexDuration]),
      common.band([panelApplyRaftWriteLogsDuration]),
      common.band([panelRegionWriteDurationDecode, panelRegionWriteDurationWriteBlocks]),
      common.band([panelApplyRaftWriteLogsDurationHeatmap, panelApplyRaftAdminLogsDurationHeatmap]),
      common.band([panelRaftEventsQps, panelRaftFrequentEventsQps]),
      common.band([panelRaftLogGapHeatmap, panelRaftEntryBatchSizeHeatmap]),
      common.band([panelRegionSizeByEventHeatmap, panelBigWriteToRegionSizeHeatmap]),
      common.band([panelWriteCommittedSizeHeatmap]),
      common.band([panelRaftEagerGcOps, panelRaftEagerGcDuration]),
      common.band([panelKeysFlow]),
      common.band([panelRaftThroughput]),
      common.band([panelUpstreamLatencyHeatmap, panelUpstreamLatency]),
      common.band([{ panel: panelLogReplicationRejected, w: 12 }])
    ],
  ),
}
