// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Threads CPU');

local panelSstImportService = graphPanel.new(
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

local panelSstApply = common.cpuWithLimitPanel(
  'SST Apply',
  'apply_low_.*',
  description='Involved when importing data.',
);

local panelRegionTask = common.cpuWithLimitPanel(
  'Region Task',
  'region_task.*',
  legend='{{name}} {{instance}}',
);

local panelRegionWorker = common.cpuWithLimitPanel(
  'Region Worker',
  'region_worker.*',
  legend='{{name}} {{instance}}',
);

local panelRaftStore = common.cpuWithLimitPanel(
  'Raft Store',
  'raftstore_.*',
  legend='{{name}} {{instance}}',
);

local panelApplyWorker = common.cpuWithLimitPanel(
  'Apply Worker',
  'apply_.*',
  legend='{{name}} {{instance}}',
);

local panelStorageBackgroundSmallTasks = common.cpuWithLimitPanel(
  'Storage Background (Small Tasks)',
  'bg_\\d+',
  legend='{{name}} {{instance}}',
);

local panelStorageBackgroundLargeTasks = common.cpuWithLimitPanel(
  'Storage Background (Large Tasks)',
  'bg_block_\\d+',
  legend='{{name}} {{instance}}',
);

local panelManualCompaction = common.cpuWithLimitPanel(
  'Manual Compaction',
  'm_compact_pool',
  description='Involved when manually compacting the data.',
  legend='{{name}} {{instance}}',
);

local panelGrpcAsyncServer = common.cpuWithLimitPanel(
  'GRPC Async Server',
  'async_poller.*',
  legend='{{name}} {{instance}}',
);

local panelGrpcAsyncClient = common.cpuWithLimitPanel(
  'GRPC Async Client',
  'GRPCComp.*',
  legend='{{name}} {{instance}}',
);

local panelFapBuilder = common.cpuWithLimitPanel(
  'FAP builder',
  'fap_builder.*',
  legend='{{name}} {{instance}}',
);

local panelSnapshotSender = common.cpuWithLimitPanel(
  'Snapshot Sender',
  'snap_sender.*',
  legend='{{name}} {{instance}}',
);

local panelSegmentScheduler = common.cpuWithLimitPanel(
  'Segment Scheduler',
  'segment_sched.*',
  legend='{{name}} {{instance}}',
);

local panelLocalIndexPool = common.cpuWithLimitPanel(
  'Local Index Pool',
  'LocalIndexPool*',
  legend='pool-{{instance}}',
);

local panelSegmentReader = common.cpuWithLimitPanel(
  'Segment Reader',
  'SegmentReader.*',
  legend='{{name}} {{instance}}',
);

{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelSstImportService, panelSstApply]),
      common.band([panelRegionTask, panelRegionWorker]),
      common.band([panelRaftStore, panelApplyWorker]),
      common.band([panelStorageBackgroundSmallTasks, panelStorageBackgroundLargeTasks]),
      common.band([panelManualCompaction, panelGrpcAsyncServer]),
      common.band([panelGrpcAsyncClient, panelFapBuilder]),
      common.band([panelSnapshotSender, panelSegmentScheduler]),
      common.band([panelLocalIndexPool, panelSegmentReader])
    ],
  ),
}
