// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Disaggregated-Write');

local checkpoint_Upload_DurationP = graphPanel.new(
  title='Checkpoint Upload Duration',
  datasource=common.datasource,
  description='PageStorage Checkpoint Duration',
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
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_checkpoint_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type, $additional_groupby) / 1000000000)',
    legendFormat='{{type}} {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_storage_checkpoint_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, $additional_groupby))',
    legendFormat='{{type}}-999 {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_checkpoint_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, $additional_groupby))',
    legendFormat='{{type}}-99 {{$additional_groupby}}',
    hide=true,
  )
);

local checkpoint_Upload_flowP = graphPanel.new(
  title='Checkpoint Upload flow',
  datasource=common.datasource,
  description='The flow of checkpoint operations',
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
);

local checkpoint_Upload_keys_speed_by_type_allP = graphPanel.new(
  title='Checkpoint Upload keys speed by type (all)',
  datasource=common.datasource,
  description='The keys of checkpoint operations. All keys are uploaded in the checkpoint. Grouped by key types.',
  formatY1='ops',
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
    'sum(rate(tiflash_storage_checkpoint_keys_by_types{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
  )
);

local checkpoint_Upload_flow_by_type_incremental_compactionP = graphPanel.new(
  title='Checkpoint Upload flow by type (incremental+compaction)',
  datasource=common.datasource,
  description='The flow of checkpoint operations. Group by key types',
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
    'sum(rate(tiflash_storage_checkpoint_flow_by_types{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
  )
);

local remote_File_NumP = graphPanel.new(
  title='Remote File Num',
  datasource=common.datasource,
  description='The number of files of owned by each TiFlash node',
  formatY1='short',
  formatY2='percentunit',
  min='0',
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
);

local remote_Store_UsageP = graphPanel.new(
  title='Remote Store Usage',
  datasource=common.datasource,
  description='The remote store usage owned by each TiFlash node',
  formatY1='bytes',
  formatY2='percentunit',
  min='0',
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
.addSeriesOverride({ alias: '/size/', linewidth: 3 });

local remote_Object_Lock_Request_QPSP = graphPanel.new(
  title='Remote Object Lock Request QPS',
  datasource=common.datasource,
  formatY1='none',
  formatY2='none',
  min='0',
  fill=0,
  nullPointMode='null',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideEmpty=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_disaggregated_object_lock_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
);

local remote_Object_Lock_DurationP = graphPanel.new(
  title='Remote Object Lock Duration',
  datasource=common.datasource,
  formatY1='s',
  formatY2='short',
  min='0',
  fill=0,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_disaggregated_object_lock_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, $additional_groupby))',
    legendFormat='99%-{{type}} {{$additional_groupby}}',
  )
);

local remote_Store_SummaryP = graphPanel.new(
  title='Remote Store Summary',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
  min='0',
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
);

local remote_GC_Duration_BreakdownP = graphPanel.new(
  title='Remote GC Duration Breakdown',
  datasource=common.datasource,
  formatY1='s',
  formatY2='s',
  min='0',
  fill=0,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_storage_s3_gc_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, $additional_groupby))',
    legendFormat='99%-{{type}} {{$additional_groupby}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_s3_gc_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, $additional_groupby))',
    legendFormat='90%-{{type}} {{$additional_groupby}}',
  )
)
.addSeriesOverride({ alias: '/total/', yaxis: 2 })
.addSeriesOverride({ alias: '/one_store/', yaxis: 2 })
.addSeriesOverride({ alias: '/clean_locks/', yaxis: 2 });

local remote_GC_StatusP = graphPanel.new(
  title='Remote GC Status',
  datasource=common.datasource,
  formatY1='short',
  formatY2='short',
  min='0',
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
);

local local_Lock_Manager_statusP = graphPanel.new(
  title='Local Lock Manager status',
  datasource=common.datasource,
  formatY1='short',
  formatY2='short',
  min='0',
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
);

local local_Lock_Manager_QPSP = graphPanel.new(
  title='Local Lock Manager QPS',
  datasource=common.datasource,
  formatY1='none',
  formatY2='none',
  min='0',
  fill=0,
  nullPointMode='null',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideEmpty=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_s3_lock_mgr_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
);

local fAP_resultP = graphPanel.new(
  title='FAP result',
  datasource=common.datasource,
  formatY1='ops',
  formatY2='percentunit',
  min='0',
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
.addSeriesOverride({ alias: '/hit_ratio/', yaxis: 2 });

local fAP_stateP = graphPanel.new(
  title='FAP state',
  datasource=common.datasource,
  formatY1='ops',
  formatY2='percentunit',
  min='0',
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
.addSeriesOverride({ alias: '/hit_ratio/', yaxis: 2 });

local fAP_time_by_stageP = graphPanel.new(
  title='FAP time by stage',
  datasource=common.datasource,
  formatY1='s',
  formatY2='percentunit',
  min='0',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(round(1000000000*rate(tiflash_fap_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type, $additional_groupby) / 1000000000)',
    legendFormat='{{type}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/hit_ratio/', yaxis: 2 });

local fAP_no_match_reasonP = graphPanel.new(
  title='FAP no match reason',
  datasource=common.datasource,
  formatY1='ops',
  formatY2='percentunit',
  min='0',
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
.addSeriesOverride({ alias: '/hit_ratio/', yaxis: 2 });


{
  row: rowObj
  .addPanel(checkpoint_Upload_DurationP, gridPos=common.pos(12, 8, x=0, y=33))
  .addPanel(checkpoint_Upload_flowP, gridPos=common.pos(12, 8, x=12, y=33))
  .addPanel(checkpoint_Upload_keys_speed_by_type_allP, gridPos=common.pos(12, 8, x=0, y=41))
  .addPanel(checkpoint_Upload_flow_by_type_incremental_compactionP, gridPos=common.pos(12, 8, x=12, y=41))
  .addPanel(remote_File_NumP, gridPos=common.pos(12, 8, x=0, y=49))
  .addPanel(remote_Store_UsageP, gridPos=common.pos(12, 8, x=12, y=49))
  .addPanel(remote_Object_Lock_Request_QPSP, gridPos=common.pos(12, 8, x=0, y=57))
  .addPanel(remote_Object_Lock_DurationP, gridPos=common.pos(12, 8, x=12, y=57))
  .addPanel(remote_Store_SummaryP, gridPos=common.pos(8, 8, x=0, y=65))
  .addPanel(remote_GC_Duration_BreakdownP, gridPos=common.pos(9, 8, x=8, y=65))
  .addPanel(remote_GC_StatusP, gridPos=common.pos(7, 8, x=17, y=65))
  .addPanel(local_Lock_Manager_statusP, gridPos=common.pos(12, 8, x=0, y=73))
  .addPanel(local_Lock_Manager_QPSP, gridPos=common.pos(12, 8, x=12, y=73))
  .addPanel(fAP_resultP, gridPos=common.pos(12, 8, x=0, y=81))
  .addPanel(fAP_stateP, gridPos=common.pos(12, 8, x=12, y=81))
  .addPanel(fAP_time_by_stageP, gridPos=common.pos(12, 8, x=0, y=89))
  .addPanel(fAP_no_match_reasonP, gridPos=common.pos(12, 8, x=12, y=89))
  ,
  panels: [
    { panel: checkpoint_Upload_DurationP, w: 12, h: 8, x: 0, y: 33 },
    { panel: checkpoint_Upload_flowP, w: 12, h: 8, x: 12, y: 33 },
    { panel: checkpoint_Upload_keys_speed_by_type_allP, w: 12, h: 8, x: 0, y: 41 },
    { panel: checkpoint_Upload_flow_by_type_incremental_compactionP, w: 12, h: 8, x: 12, y: 41 },
    { panel: remote_File_NumP, w: 12, h: 8, x: 0, y: 49 },
    { panel: remote_Store_UsageP, w: 12, h: 8, x: 12, y: 49 },
    { panel: remote_Object_Lock_Request_QPSP, w: 12, h: 8, x: 0, y: 57 },
    { panel: remote_Object_Lock_DurationP, w: 12, h: 8, x: 12, y: 57 },
    { panel: remote_Store_SummaryP, w: 8, h: 8, x: 0, y: 65 },
    { panel: remote_GC_Duration_BreakdownP, w: 9, h: 8, x: 8, y: 65 },
    { panel: remote_GC_StatusP, w: 7, h: 8, x: 17, y: 65 },
    { panel: local_Lock_Manager_statusP, w: 12, h: 8, x: 0, y: 73 },
    { panel: local_Lock_Manager_QPSP, w: 12, h: 8, x: 12, y: 73 },
    { panel: fAP_resultP, w: 12, h: 8, x: 0, y: 81 },
    { panel: fAP_stateP, w: 12, h: 8, x: 12, y: 81 },
    { panel: fAP_time_by_stageP, w: 12, h: 8, x: 0, y: 89 },
    { panel: fAP_no_match_reasonP, w: 12, h: 8, x: 12, y: 89 }
  ],
}
