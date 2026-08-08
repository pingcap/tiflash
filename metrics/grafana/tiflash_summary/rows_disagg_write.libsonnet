// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Disaggregated-Write');

local panelCheckpointUploadDuration = common.durationPanel(
  'Checkpoint Upload Duration',
  'tiflash_storage_checkpoint_seconds_bucket',
  by=['type'],
  legend='{{type}}-%s {{$additional_groupby}}',
  description='PageStorage Checkpoint Duration',
);

local panelCheckpointUploadFlow = graphPanel.new(
  title='Checkpoint Upload flow',
  datasource=common.datasource,
  description='The flow of checkpoint operations',
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
    'sum(rate(tiflash_storage_checkpoint_flow{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="incremental"}[1m])) by ($additional_groupby)',
    legendFormat='incremental {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_checkpoint_flow{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="compaction"}[1m])) by ($additional_groupby)',
    legendFormat='compaction {{$additional_groupby}}',
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

local panelCheckpointUploadKeysSpeedByTypeAll = common.opsPanel(
  'Checkpoint Upload keys speed by type (all)',
  'tiflash_storage_checkpoint_keys_by_types',
  by=['type', '$additional_groupby'],
  legend='{{type}} {{$additional_groupby}}',
  description='The keys of checkpoint operations. All keys are uploaded in the checkpoint. Grouped by key types.',
  fill=1,
  yRight='short',
);

local panelCheckpointUploadFlowByTypeIncrementalCompaction = graphPanel.new(
  title='Checkpoint Upload flow by type (incremental+compaction)',
  datasource=common.datasource,
  description='The flow of checkpoint operations. Group by key types',
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
    'sum(rate(tiflash_storage_checkpoint_flow_by_types{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
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

local panelRemoteFileNum = graphPanel.new(
  title='Remote File Num',
  datasource=common.datasource,
  description='The number of files of owned by each TiFlash node',
  fill=0,
  nullPointMode='null as zero',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="num_files"}) by (instance)',
    legendFormat='checkpoint_data-{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
  max='1.1',
);

local panelRemoteStoreUsage = graphPanel.new(
  title='Remote Store Usage',
  datasource=common.datasource,
  description='The remote store usage owned by each TiFlash node',
  fill=0,
  nullPointMode='null as zero',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="total_size"}) by (instance)',
    legendFormat='remote_size-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="valid_size"}) by (instance)',
    legendFormat='valid_size-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum((tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="valid_size"}) / (tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="total_size"})) by (instance)',
    legendFormat='valid_rate-{{instance}}',
    hide=true,
  )
)
.addSeriesOverride({ alias: '/^valid_rate/', yaxis: 2 })
.addSeriesOverride({ alias: '/size/', linewidth: 3 })
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
  max='1.1',
);

local panelRemoteObjectLockRequestQps = common.opsPanel(
  'Remote Object Lock Request QPS',
  'tiflash_disaggregated_object_lock_request_count',
  by=['type', '$additional_groupby'],
  legend='{{type}} {{$additional_groupby}}',
  yLeft='none',
);

local panelRemoteObjectLockDuration = common.durationPanel(
  'Remote Object Lock Duration',
  'tiflash_disaggregated_object_lock_request_duration_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
);

local panelRemoteStoreSummary = graphPanel.new(
  title='Remote Store Summary',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_hideZero=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_storage_s3_store_summary_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, store_id,type)',
    legendFormat='store-{{store_id}}-{{type}}',
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

local panelRemoteGcDurationBreakdown = common.durationPanel(
  'Remote GC Duration Breakdown',
  'tiflash_storage_s3_gc_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
)
.addSeriesOverride({ alias: '/total/', yaxis: 2 })
.addSeriesOverride({ alias: '/one_store/', yaxis: 2 })
.addSeriesOverride({ alias: '/clean_locks/', yaxis: 2 });

local panelRemoteGcStatus = graphPanel.new(
  title='Remote GC Status',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_hideZero=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_storage_s3_gc_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,type)',
    legendFormat='{{instance}}-{{type}}',
  )
)
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
)
.addYaxis(
  format='short',
);

local panelLocalLockManagerStatus = graphPanel.new(
  title='Local Lock Manager status',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_hideZero=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_storage_s3_lock_mgr_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,type)',
    legendFormat='{{instance}}-{{type}}',
  )
)
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
)
.addYaxis(
  format='short',
);

local panelLocalLockManagerQps = common.opsPanel(
  'Local Lock Manager QPS',
  'tiflash_storage_s3_lock_mgr_counter',
  by=['type', '$additional_groupby'],
  legend='{{type}} {{$additional_groupby}}',
  yLeft='none',
);

local panelFapResult = graphPanel.new(
  title='FAP result',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_fap_task_result{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/hit_ratio/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
);

local panelFapState = graphPanel.new(
  title='FAP state',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_fap_task_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/hit_ratio/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
);

local panelFapTimeByStage = common.durationPanel(
  'FAP time by stage',
  'tiflash_fap_task_duration_seconds_bucket',
  by=['type'],
  legend='{{type}}-%s {{$additional_groupby}}',
)
.addSeriesOverride({ alias: '/hit_ratio/', yaxis: 2 });

local panelFapNoMatchReason = graphPanel.new(
  title='FAP no match reason',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_fap_nomatch_reason{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/hit_ratio/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelCheckpointUploadDuration, panelCheckpointUploadFlow]),
      common.band([panelCheckpointUploadKeysSpeedByTypeAll, panelCheckpointUploadFlowByTypeIncrementalCompaction]),
      common.band([panelRemoteFileNum, panelRemoteStoreUsage]),
      common.band([panelRemoteObjectLockRequestQps, panelRemoteObjectLockDuration]),
      common.band([panelRemoteStoreSummary, panelRemoteGcDurationBreakdown, panelRemoteGcStatus]),
      common.band([panelLocalLockManagerStatus, panelLocalLockManagerQps]),
      common.band([panelFapResult, panelFapState]),
      common.band([panelFapTimeByStage, panelFapNoMatchReason])
    ],
  ),
}
