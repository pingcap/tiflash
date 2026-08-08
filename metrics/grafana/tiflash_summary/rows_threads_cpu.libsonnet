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

local sST_ApplyP = common.cpuWithLimitPanel(
  'SST Apply',
  'apply_low_.*',
  description='Involved when importing data.',
);

local region_TaskP = common.cpuWithLimitPanel(
  'Region Task',
  'region_task.*',
  legend='{{name}} {{instance}}',
);

local region_WorkerP = common.cpuWithLimitPanel(
  'Region Worker',
  'region_worker.*',
  legend='{{name}} {{instance}}',
);

local raft_StoreP = common.cpuWithLimitPanel(
  'Raft Store',
  'raftstore_.*',
  legend='{{name}} {{instance}}',
);

local apply_WorkerP = common.cpuWithLimitPanel(
  'Apply Worker',
  'apply_.*',
  legend='{{name}} {{instance}}',
);

local storage_Background_Small_TasksP = common.cpuWithLimitPanel(
  'Storage Background (Small Tasks)',
  'bg_\\d+',
  legend='{{name}} {{instance}}',
);

local storage_Background_Large_TasksP = common.cpuWithLimitPanel(
  'Storage Background (Large Tasks)',
  'bg_block_\\d+',
  legend='{{name}} {{instance}}',
);

local manual_CompactionP = common.cpuWithLimitPanel(
  'Manual Compaction',
  'm_compact_pool',
  description='Involved when manually compacting the data.',
  legend='{{name}} {{instance}}',
);

local gRPC_Async_ServerP = common.cpuWithLimitPanel(
  'GRPC Async Server',
  'async_poller.*',
  legend='{{name}} {{instance}}',
);

local gRPC_Async_ClientP = common.cpuWithLimitPanel(
  'GRPC Async Client',
  'GRPCComp.*',
  legend='{{name}} {{instance}}',
);

local fAP_builderP = common.cpuWithLimitPanel(
  'FAP builder',
  'fap_builder.*',
  legend='{{name}} {{instance}}',
);

local snapshot_SenderP = common.cpuWithLimitPanel(
  'Snapshot Sender',
  'snap_sender.*',
  legend='{{name}} {{instance}}',
);

local segment_SchedulerP = common.cpuWithLimitPanel(
  'Segment Scheduler',
  'segment_sched.*',
  legend='{{name}} {{instance}}',
);

local local_Index_PoolP = common.cpuWithLimitPanel(
  'Local Index Pool',
  'LocalIndexPool*',
  legend='pool-{{instance}}',
);

local segment_ReaderP = common.cpuWithLimitPanel(
  'Segment Reader',
  'SegmentReader.*',
  legend='{{name}} {{instance}}',
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
