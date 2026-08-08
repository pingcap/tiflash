// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
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
)
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

local sST_ApplyP = graphPanel.new(
  title='SST Apply',
  datasource=common.datasource,
  description='Involved when importing data.',
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local region_TaskP = graphPanel.new(
  title='Region Task',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local region_WorkerP = graphPanel.new(
  title='Region Worker',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local raft_StoreP = graphPanel.new(
  title='Raft Store',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local apply_WorkerP = graphPanel.new(
  title='Apply Worker',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local storage_Background_Small_TasksP = graphPanel.new(
  title='Storage Background (Small Tasks)',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local storage_Background_Large_TasksP = graphPanel.new(
  title='Storage Background (Large Tasks)',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local manual_CompactionP = graphPanel.new(
  title='Manual Compaction',
  datasource=common.datasource,
  description='Involved when manually compacting the data.',
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local gRPC_Async_ServerP = graphPanel.new(
  title='GRPC Async Server',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local gRPC_Async_ClientP = graphPanel.new(
  title='GRPC Async Client',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local fAP_builderP = graphPanel.new(
  title='FAP builder',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local snapshot_SenderP = graphPanel.new(
  title='Snapshot Sender',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local segment_SchedulerP = graphPanel.new(
  title='Segment Scheduler',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local local_Index_PoolP = graphPanel.new(
  title='Local Index Pool',
  datasource=common.datasource,
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
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"LocalIndexSched*", instance=~"$tiflash_role"}[1m]))',
    legendFormat='sched-{{instance}}',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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

local segment_ReaderP = graphPanel.new(
  title='Segment Reader',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
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


{
  row: common.buildRow(
    rowObj,
    [
      common.band([sST_Import_ServiceP, sST_ApplyP]),
      common.band([region_TaskP, region_WorkerP]),
      common.band([raft_StoreP, apply_WorkerP]),
      common.band([storage_Background_Small_TasksP, storage_Background_Large_TasksP]),
      common.band([manual_CompactionP, gRPC_Async_ServerP]),
      common.band([gRPC_Async_ClientP, fAP_builderP]),
      common.band([snapshot_SenderP, segment_SchedulerP]),
      common.band([local_Index_PoolP, segment_ReaderP])
    ],
  ),
}
