// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Threads CPU');

local sST_Import_ServiceP = graphPanel.new(
  title='SST Import Service',
  datasource=common.datasource,
  description='Involved when importing data.',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"sst_importer.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{instance}}',
  )
);

local sST_ApplyP = graphPanel.new(
  title='SST Apply',
  datasource=common.datasource,
  description='Involved when importing data.',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"apply_low_.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"apply_low_.*", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local region_TaskP = graphPanel.new(
  title='Region Task',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"region_task.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"region_task.*", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local region_WorkerP = graphPanel.new(
  title='Region Worker',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"region_worker.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"region_worker.*", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local raft_StoreP = graphPanel.new(
  title='Raft Store',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"raftstore_.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"raftstore_.*", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local apply_WorkerP = graphPanel.new(
  title='Apply Worker',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"apply_.*", name!~"apply_low_.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"apply_.*", name!~"apply_low_.*", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local storage_Background_Small_TasksP = graphPanel.new(
  title='Storage Background (Small Tasks)',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"bg_\\\\d+", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"bg_\\\\d+", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local storage_Background_Large_TasksP = graphPanel.new(
  title='Storage Background (Large Tasks)',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"bg_block_\\\\d+", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"bg_block_\\\\d+", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local manual_CompactionP = graphPanel.new(
  title='Manual Compaction',
  datasource=common.datasource,
  description='Involved when manually compacting the data.',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"m_compact_pool", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"m_compact_pool", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local gRPC_Async_ServerP = graphPanel.new(
  title='GRPC Async Server',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"async_poller.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"async_poller.*", instance=~"$tiflash_role"}) < sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance) or sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local gRPC_Async_ClientP = graphPanel.new(
  title='GRPC Async Client',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"GRPCComp.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"GRPCComp.*", instance=~"$tiflash_role"}) < sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance) or sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local fAP_builderP = graphPanel.new(
  title='FAP builder',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"fap_builder.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"GRPCComp.*", instance=~"$tiflash_role"}) < sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance) or sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local snapshot_SenderP = graphPanel.new(
  title='Snapshot Sender',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"snap_sender.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"snap_sender.*", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local segment_SchedulerP = graphPanel.new(
  title='Segment Scheduler',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"segment_sched.*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"segment_sched.*", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

local local_Index_PoolP = graphPanel.new(
  title='Local Index Pool',
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"LocalIndexPool*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='pool-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"LocalIndexPool*", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addTarget(
  prometheus.target(
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"LocalIndexSched*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='sched-{{instance}}',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });

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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"SegmentReader.*", instance=~"$tiflash_role", instance=~"$proxy_instance"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"SegmentReader.*", instance=~"$tiflash_role", instance=~"$proxy_instance"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' });


{
  row: rowObj
  .addPanel(sST_Import_ServiceP, gridPos=common.pos(12, 7))
  .addPanel(sST_ApplyP, gridPos=common.pos(12, 7))
  .addPanel(region_TaskP, gridPos=common.pos(12, 7))
  .addPanel(region_WorkerP, gridPos=common.pos(12, 7))
  .addPanel(raft_StoreP, gridPos=common.pos(12, 7))
  .addPanel(apply_WorkerP, gridPos=common.pos(12, 7))
  .addPanel(storage_Background_Small_TasksP, gridPos=common.pos(12, 7))
  .addPanel(storage_Background_Large_TasksP, gridPos=common.pos(12, 7))
  .addPanel(manual_CompactionP, gridPos=common.pos(12, 7))
  .addPanel(gRPC_Async_ServerP, gridPos=common.pos(12, 7))
  .addPanel(gRPC_Async_ClientP, gridPos=common.pos(12, 7))
  .addPanel(fAP_builderP, gridPos=common.pos(12, 7))
  .addPanel(snapshot_SenderP, gridPos=common.pos(12, 7))
  .addPanel(segment_SchedulerP, gridPos=common.pos(12, 7))
  .addPanel(local_Index_PoolP, gridPos=common.pos(12, 7))
  .addPanel(segment_ReaderP, gridPos=common.pos(12, 7))
  ,
  panels: [
    { panel: sST_Import_ServiceP, w: 12, h: 7 },
    { panel: sST_ApplyP, w: 12, h: 7 },
    { panel: region_TaskP, w: 12, h: 7 },
    { panel: region_WorkerP, w: 12, h: 7 },
    { panel: raft_StoreP, w: 12, h: 7 },
    { panel: apply_WorkerP, w: 12, h: 7 },
    { panel: storage_Background_Small_TasksP, w: 12, h: 7 },
    { panel: storage_Background_Large_TasksP, w: 12, h: 7 },
    { panel: manual_CompactionP, w: 12, h: 7 },
    { panel: gRPC_Async_ServerP, w: 12, h: 7 },
    { panel: gRPC_Async_ClientP, w: 12, h: 7 },
    { panel: fAP_builderP, w: 12, h: 7 },
    { panel: snapshot_SenderP, w: 12, h: 7 },
    { panel: segment_SchedulerP, w: 12, h: 7 },
    { panel: local_Index_PoolP, w: 12, h: 7 },
    { panel: segment_ReaderP, w: 12, h: 7 }
  ],
}
