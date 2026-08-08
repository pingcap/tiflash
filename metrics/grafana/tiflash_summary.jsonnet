// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

local grafana = import 'grafonnet/grafana.libsonnet';
local dashboard = grafana.dashboard;
local template = grafana.template;
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'tiflashnet/common.libsonnet';

// --- Row: Server ---
local rowServer = (
  local rowObj = row.new(collapse=true, title='Server');

  local panelStoreSize = graphPanel.new(
    title='Store size',
    datasource=common.datasource,
    description='The storage size per TiFlash instance.\n(Not including some disk usage of TiFlash-Proxy by now)',
    fill=5,
    linewidth=0,
    nullPointMode='null as zero',
    stack=true,
    decimals=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_hideEmpty=true,
    legend_hideZero=true,
    legend_sort='current',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_StoreSizeUsed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~""}) by (instance)',
      legendFormat='{{instance}}-local',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_StoreSizeUsedRemote{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='{{instance}}-remote',
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

  local panelAvailableSize = graphPanel.new(
    title='Available size',
    datasource=common.datasource,
    description='The available capacity size per TiFlash instance',
    fill=5,
    linewidth=0,
    nullPointMode='null as zero',
    stack=true,
    decimals=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_sort='current',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_StoreSizeAvailable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='{{instance}}',
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

  local panelCapacitySize = graphPanel.new(
    title='Capacity size',
    datasource=common.datasource,
    description='The capacity size per TiFlash instance',
    fill=5,
    linewidth=0,
    nullPointMode='null as zero',
    stack=true,
    decimals=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_sort='current',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_StoreSizeCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='{{instance}}',
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

  local panelUptime = graphPanel.new(
    title='Uptime',
    datasource=common.datasource,
    description='TiFlash uptime since last restart',
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_sort='current',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_asynchronous_metric_Uptime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='{{instance}}',
    )
  )
  .addSeriesOverride({ alias: 'total', fill: 0, lines: false })
  .resetYaxes()
  .addYaxis(
    format='dtdurations',
  )
  .addYaxis(
    format='short',
  );

  local panelRegion = graphPanel.new(
    title='Region',
    datasource=common.datasource,
    description='The number of Regions on each TiFlash instance',
    fill=0,
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
      'sum(tiflash_proxy_tikv_raftstore_region_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="region", instance=~"$proxy_instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_proxy_tikv_raftstore_hibernated_peer_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}) by (instance, state)',
      legendFormat='{{instance}}-{{state}}',
      hide=true,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
  )
  .addYaxis(
    format='short',
    show=false,
  );

  local panelCpuUsage = graphPanel.new(
    title='CPU Usage',
    datasource=common.datasource,
    description='TiFlash CPU usage calculated with process CPU running seconds.',
    fill=0,
    nullPointMode='null',
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
      'rate(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])',
      legendFormat='{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='limit-{{instance}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 })
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

  local panelMemory = graphPanel.new(
    title='Memory',
    datasource=common.datasource,
    description='The memory usage per TiFlash instance',
    fill=0,
    nullPointMode='null',
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
      'tiflash_proxy_process_resident_memory_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
      legendFormat='{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_MemoryCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='limit-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_jemalloc_retained{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='retained',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_jemalloc_mapped{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='mapped',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_jemalloc_resident{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='resident',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_jemalloc_allocated{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='allocated',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_jemalloc_active{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='active',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_jemalloc_metadata_thp{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='metadata_thp',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_jemalloc_metadata{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='metadata',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_mimalloc_current_rss{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='mimalloc_rss',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_mimalloc_current_commit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='mimalloc_commit',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_mmap_alive{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='mmap',
      intervalFactor=1,
      hide=true,
    )
  )
  .addSeriesOverride({ alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 })
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
  )
  .addYaxis(
    format='short',
    show=false,
  );

  local panelIoThroughput = graphPanel.new(
    title='IO Throughput',
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
  )
  .addTarget(
    prometheus.target(
      'sum by (instance) (irate(tiflash_proxy_threads_io_bytes_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='{{instance}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
    decimals=0,
  )
  .addYaxis(
    format='short',
    show=false,
  );

  local panelRemoteStoreSummaryDisaggArch = graphPanel.new(
    title='Remote Store Summary (Disagg arch)',
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

  common.buildRow(
      rowObj,
      [
        common.band([panelStoreSize, panelAvailableSize, panelCapacitySize]),
        common.band([panelUptime, panelRegion]),
        common.band([panelCpuUsage, panelMemory]),
        common.band([panelIoThroughput, panelRemoteStoreSummaryDisaggArch])
      ],
    )
);

// --- Row: Threads CPU ---
local rowThreadsCpu = (
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
  common.buildRow(
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
    )
);

// --- Row: Threads ---
local rowThreads = (
  local rowObj = row.new(collapse=true, title='Threads');

  local panelThreadsState = graphPanel.new(
    title='Threads state',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    points=true,
    pointradius=2,
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
      'sum(tiflash_proxy_threads_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}) by (instance, state)',
      legendFormat='{{instance}}-{{state}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_proxy_threads_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='{{instance}}-total',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='none',
  )
  .addYaxis(
    format='short',
  );

  local panelThreadsIo = graphPanel.new(
    title='Threads IO',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    points=true,
    pointradius=2,
    decimals=1,
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
      'sum(rate(tiflash_proxy_threads_io_bytes_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[30s])) by (name, io, $additional_groupby) > 1024',
      legendFormat='{{name}}-{{io}} {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='Bps',
  )
  .addYaxis(
    format='short',
  );

  local panelThreadVoluntaryContextSwitches = graphPanel.new(
    title='Thread Voluntary Context Switches',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    points=true,
    pointradius=2,
    decimals=1,
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
      'sum(rate(tiflash_proxy_thread_voluntary_context_switches{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[30s])) by (instance, name) > 200',
      legendFormat='{{instance}} - {{name}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='none',
  )
  .addYaxis(
    format='short',
  );

  local panelThreadNonvoluntaryContextSwitches = graphPanel.new(
    title='Thread Nonvoluntary Context Switches',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    points=true,
    pointradius=2,
    decimals=1,
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
      'sum(rate(tiflash_proxy_thread_nonvoluntary_context_switches{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[30s])) by (instance, name) > 50',
      legendFormat='{{instance}} - {{name}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='none',
  )
  .addYaxis(
    format='short',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelThreadsState, panelThreadsIo]),
        common.band([panelThreadVoluntaryContextSwitches, panelThreadNonvoluntaryContextSwitches])
      ],
    )
);

// --- Row: Coprocessor ---
local rowCoprocessor = (
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

  common.buildRow(
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
    )
);

// --- Row: Task Scheduler ---
local rowTaskScheduler = (
  local rowObj = row.new(collapse=true, title='Task Scheduler');

  local panelMinTso = graphPanel.new(
    title='Min TSO',
    datasource=common.datasource,
    description='the min_tso of each instance',
    fill=1,
    nullPointMode='null',
    points=true,
    lines=false,
    pointradius=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="min_tso"}) by (instance, resource_group)',
      legendFormat='{{instance}}-{{resource_group}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='none',
    label='TSO',
    show=false,
  )
  .addYaxis(
    format='short',
  );

  local panelEstimatedThreadUsageAndLimit = graphPanel.new(
    title='Estimated Thread Usage and Limit',
    datasource=common.datasource,
    description='estimated thread usage in min-tso scheduler, and the sort/hard limit of estimated thread in scheduler.',
    fill=0,
    nullPointMode='null as zero',
    pointradius=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="thread_soft_limit"}) by (instance, type, resource_group)',
      legendFormat='{{instance}}-{{type}}-{{resource_group}}',
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="estimated_thread_usage"}) by (instance, type, resource_group)',
      legendFormat='{{instance}}-{{type}}-{{resource_group}}',
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="thread_hard_limit"}) by (instance, type, resource_group)',
      legendFormat='{{instance}}-{{type}}-{{resource_group}}',
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="global_estimated_thread_usage"}) by (instance, type, resource_group)',
      legendFormat='{{instance}}-{{type}}-{{resource_group}}',
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="group_entry_count"}) by (instance, type)',
      legendFormat='{{instance}}-{{type}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='none',
    label='Threads',
    logBase=10,
  )
  .addYaxis(
    format='short',
  );

  local panelActiveAndWaitingQueriesCount = graphPanel.new(
    title='Active and Waiting Queries Count',
    datasource=common.datasource,
    description='the count of active/ waiting queries',
    fill=0,
    nullPointMode='null as zero',
    pointradius=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="waiting_queries_count"}) by (instance, type, resource_group)',
      legendFormat='{{instance}}-{{type}}-{{resource_group}}',
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="active_queries_count"}) by (instance, type, resource_group)',
      legendFormat='{{instance}}-{{type}}-{{resource_group}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='none',
    label='Queries',
  )
  .addYaxis(
    format='short',
  );

  local panelActiveAndWaitingTasksCount = graphPanel.new(
    title='Active and Waiting Tasks Count',
    datasource=common.datasource,
    description='the count of active/ waiting tasks',
    fill=0,
    nullPointMode='null as zero',
    pointradius=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="waiting_tasks_count"}) by (instance, type, resource_group)',
      legendFormat='{{instance}}-{{type}}-{{resource_group}}',
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="active_tasks_count"}) by (instance, type, resource_group)',
      legendFormat='{{instance}}-{{type}}-{{resource_group}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='none',
    label='Tasks',
  )
  .addYaxis(
    format='short',
  );

  local panelHardLimitExceededCount = graphPanel.new(
    title='Hard Limit Exceeded Count',
    datasource=common.datasource,
    description='the usage of estimated threads exceeded the hard limit where errors occur.',
    fill=0,
    nullPointMode='null as zero',
    pointradius=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="hard_limit_exceeded_count"}) by (instance, type, resource_group)',
      legendFormat='{{instance}}-{{resource_group}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='none',
  )
  .addYaxis(
    format='short',
  );

  local panelTaskWaitingDuration = common.durationPanel(
    'Task Waiting Duration',
    'tiflash_task_scheduler_waiting_duration_seconds_bucket',
    by=['instance', 'resource_group'],
    legend='{{instance}}-{{resource_group}}-%s',
    description='the time of waiting for schedule',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelMinTso, panelEstimatedThreadUsageAndLimit]),
        common.band([panelActiveAndWaitingQueriesCount, panelActiveAndWaitingTasksCount]),
        common.band([panelHardLimitExceededCount, panelTaskWaitingDuration])
      ],
    )
);

// --- Row: DDL ---
local rowDdl = (
  local rowObj = row.new(collapse=true, title='DDL');

  local panelSchemaInternalDdlOpm = graphPanel.new(
    title='Schema Internal DDL OPM',
    datasource=common.datasource,
    description='Executed DDL jobs per minute',
    fill=0,
    nullPointMode='null as zero',
  )
  .addTarget(
    prometheus.target(
      'avg(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
      legendFormat='{{type}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='total',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type,instance)',
      legendFormat='{{type}}-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='total-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='opm',
    min='0',
  )
  .addYaxis(
    format='none',
  );

  local panelSchemaApplyOpm = graphPanel.new(
    title='Schema Apply OPM',
    datasource=common.datasource,
    description='Executed DDL apply jobs per minute',
    fill=0,
    nullPointMode='null as zero',
  )
  .addTarget(
    prometheus.target(
      'avg(increase(tiflash_schema_trigger_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
      legendFormat='triggle-by-{{type}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='opm',
    min='0',
  )
  .addYaxis(
    format='none',
  );

  local panelSchemaApplyDuration = common.durationPanel(
    'Schema Apply Duration',
    'tiflash_schema_apply_duration_seconds_bucket',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
    extraTargets=[
      common.target(
        common.expr.sum(
          'tiflash_sync_schema_applying',
          common.selector + ', type=~"$type"',
          by=['instance'],
        ),
        'applying-{{instance}}',
      ),
    ],
    seriesOverrides=[
      common.override('/^applying/', yaxis=2),
    ],
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelSchemaInternalDdlOpm, panelSchemaApplyOpm]),
        common.band([{ panel: panelSchemaApplyDuration, w: 12 }])
      ],
    )
);

// --- Row: Imbalance read/write ---
local rowImbalanceReadWrite = (
  local rowObj = row.new(collapse=true, title='Imbalance read/write');

  local panelCpuUsageIrate = graphPanel.new(
    title='CPU Usage (irate)',
    datasource=common.datasource,
    description='TiFlash CPU usage calculated with process CPU running seconds.',
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
      'irate(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$tiflash_role"}[1m])',
      legendFormat='{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='limit-{{instance}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 })
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

  local panelSegmentReader = common.cpuWithLimitPanel(
    'Segment Reader',
    'SegmentReader.*',
    legend='{{name}} {{instance}}',
  );

  local panelRequestQpsByInstance = common.opsPanel(
    'Request QPS by instance',
    'tiflash_coprocessor_request_count',
    by=['type', 'instance'],
    legend='{{type}}-{{instance}}',
    yLeft='none',
  );

  local panelReadThroughputByInstance = graphPanel.new(
    title='Read Throughput by instance',
    datasource=common.datasource,
    description='The flow of different kinds of read operations',
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
      'sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='File Descriptor-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='Page-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='PageBackGround-{{instance}}',
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

  local panelWriteCommandOpsByInstance = graphPanel.new(
    title='Write Command OPS By Instance',
    datasource=common.datasource,
    description='The total count of different kinds of commands received',
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
      'sum(rate(tiflash_system_profile_event_DMWriteBlock{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
      legendFormat='write block-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
      legendFormat='{{type}}-{{instance}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/delete_range|ingest/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelWriteThroughputByInstance = graphPanel.new(
    title='Write Throughput By Instance',
    datasource=common.datasource,
    description='The throughput of write by instance',
    fill=0,
    nullPointMode='null',
    decimals=1,
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
      'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write"}[1m])) by (instance)',
      legendFormat='write-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ingest"}[1m])) by (instance)',
      legendFormat='ingest-{{instance}}',
    )
  )
  .addSeriesOverride({ alias: '/total/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='binBps',
    min='0',
  )
  .addYaxis(
    format='bytes',
    show=false,
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelCpuUsageIrate, panelSegmentReader]),
        common.band([panelRequestQpsByInstance, panelReadThroughputByInstance]),
        common.band([panelWriteCommandOpsByInstance, panelWriteThroughputByInstance])
      ],
    )
);

// --- Row: Memory trace ---
local rowMemoryTrace = (
  local rowObj = row.new(collapse=true, title='Memory trace');

  local panelNumberOfKeyspaces = graphPanel.new(
    title='Number of Keyspaces',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_NumKeyspace{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='keyspace-{{instance}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
  )
  .addYaxis(
    format='s',
    show=false,
  );

  local panelNumberOfPhysicalTables = graphPanel.new(
    title='Number of Physical Tables',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_NumStorageDeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='tables-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_NumIStorage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='tables-all-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
  )
  .addYaxis(
    format='s',
    show=false,
  );

  local panelNumberOfSegments = graphPanel.new(
    title='Number of Segments',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_NumSegment{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='segments-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_NumMemTable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='mem_table-{{instance}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
  )
  .addYaxis(
    format='s',
    show=false,
  );

  local panelBytesOfMemtables = graphPanel.new(
    title='Bytes of MemTables',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_BytesMemTable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='bytes-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_BytesMemTableAllocated{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='bytes-allocated-{{instance}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
  )
  .addYaxis(
    format='s',
    show=false,
  );

  local panelMarkCacheAndMinmaxIndexCacheMemoryUsage = graphPanel.new(
    title='Mark Cache and Minmax Index Cache Memory Usage',
    datasource=common.datasource,
    description='The memory usage of mark cache and minmax index cache',
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
      'tiflash_system_asynchronous_metric_MarkCacheBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='mark_cache_{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_asynchronous_metric_MinMaxIndexFiles{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='minmax_index_cache_{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_asynchronous_metric_RNMVCCIndexCacheBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='rn_mvcc_index_cache_{{instance}}',
    )
  )
  .addSeriesOverride({ alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 })
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
  )
  .addYaxis(
    format='short',
    show=false,
  );

  local panelEffectivenessOfMarkCache = graphPanel.new(
    title='Effectiveness of Mark Cache',
    datasource=common.datasource,
    description='cache misses or cache hits of mark_cache.\nBased on this infactor, we can check whether mark_cache is large enough',
    fill=1,
    nullPointMode='null',
    pointradius=2,
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_system_profile_event_MarkCacheMisses{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='mark cache misses',
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_system_profile_event_MarkCacheHits{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='mark cache hits',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='percentunit',
  )
  .addYaxis(
    format='percent',
    show=false,
  );

  local panelSchemaOfColumnFile = graphPanel.new(
    title='Schema of Column File',
    datasource=common.datasource,
    description='Information about schema of column file, to learn the memory usage of schema',
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_sort='current',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"current_size"}) by (instance)',
      legendFormat='current_size-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"hit_count"}[1m])) by (instance)',
      legendFormat='hit_count_ops-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"still_used_when_evict"}) by (instance)',
      legendFormat='still_used_when_evict-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"miss_count"}[1m])) by (instance)',
      legendFormat='miss_count_ops-{{instance}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
  )
  .addYaxis(
    format='short',
  );

  local panelReadSnapshots = graphPanel.new(
    title='Read Snapshots',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_SegmentReadTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='read_tasks-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_asynchronous_metric_MaxDTDeltaOldestSnapshotLifetime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='max_snapshot_lifetime-{{instance}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/max_snapshot_lifetime/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='short',
    min='0',
  )
  .addYaxis(
    format='s',
    min='0',
  );

  local panelMemoryByThread = graphPanel.new(
    title='Memory by thread',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
  )
  .addTarget(
    prometheus.target(
      'rate(tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}[$__interval])',
      legendFormat='{{instance}}-{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      '-rate(tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}[$__interval])',
      legendFormat='{{instance}}-{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}',
      legendFormat='{{instance}}-{{type}}-tot',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      '-tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}',
      legendFormat='{{instance}}-{{type}}-tot',
      hide=true,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
  )
  .addYaxis(
    format='short',
  );

  local panelMemoryByThreadProxy = graphPanel.new(
    title='Memory by thread (proxy)',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_hideEmpty=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'rate(tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}[$__interval])',
      legendFormat='{{instance}}-{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      '-rate(tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}[$__interval])',
      legendFormat='{{instance}}-{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}',
      legendFormat='{{instance}}-{{type}}-tot',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      '-tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}',
      legendFormat='{{instance}}-{{type}}-tot',
      hide=true,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
  )
  .addYaxis(
    format='short',
  );

  local panelMemoryByClass = graphPanel.new(
    title='Memory by class',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
  )
  .addTarget(
    prometheus.target(
      'tiflash_memory_usage_by_class{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='{{instance}}-{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      'rate(tiflash_memory_usage_by_class{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__interval])',
      legendFormat='{{instance}}-{{type}}-rate',
      hide=true,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
  )
  .addYaxis(
    format='short',
  );

  local panelKvstoreMemory = graphPanel.new(
    title='KVStore memory',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_MemoryTrackingKVStore{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='{{instance}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
  )
  .addYaxis(
    format='short',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelNumberOfKeyspaces, panelNumberOfPhysicalTables]),
        common.band([panelNumberOfSegments, panelBytesOfMemtables]),
        common.band([panelMarkCacheAndMinmaxIndexCacheMemoryUsage, panelEffectivenessOfMarkCache]),
        common.band([panelSchemaOfColumnFile, panelReadSnapshots]),
        common.band([panelMemoryByThread, panelMemoryByThreadProxy]),
        common.band([panelMemoryByClass, panelKvstoreMemory])
      ],
    )
);

// --- Row: Columnar Storage ---
local rowColumnarStorage = (
  local rowObj = row.new(collapse=true, title='Columnar Storage');

  local panelIaUsage = graphPanel.new(
    title='IA usage',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_proxy_kv_engine_ia_main_queue_capacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
      legendFormat='capacity-main-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_proxy_kv_engine_ia_small_queue_capacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
      legendFormat='capacity-small-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_proxy_kv_engine_ia_manager_segments_memory_capacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
      legendFormat='capacity-segments-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_proxy_kv_engine_ia_manager_segments_memory_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
      legendFormat='segments-mem-size-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_proxy_kv_engine_ia_manager_segments_disk_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
      legendFormat='segments-disk-size-{{instance}}',
    )
  )
  .addSeriesOverride({ alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 })
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
  )
  .addYaxis(
    format='short',
    show=false,
  );

  local panelIaSegmentsMemoryWait = common.durationPanel(
    'IA Segments Memory Wait',
    'tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket',
    selector=common.proxySelector,
  );

  local panelIaSegmentRemoteReadCache = graphPanel.new(
    title='IA Segment Remote Read Cache',
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
      'sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_cache_hit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by ($additional_groupby)',
      legendFormat='cache-hit {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_cache_miss{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by ($additional_groupby)',
      legendFormat='cache-miss {{$additional_groupby}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelIaSegmentsRemoteReadDuration = common.durationPanel(
    'IA Segments Remote Read Duration',
    'tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket',
    selector=common.proxySelector,
  );

  local panelColumnarfileCache = graphPanel.new(
    title='ColumnarFile Cache',
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
      'sum(rate(tiflash_proxy_kv_engine_columnar_file_cache_hit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by ($additional_groupby)',
      legendFormat='file-cache-hit {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_proxy_kv_engine_columnar_file_cache_miss{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by ($additional_groupby)',
      legendFormat='file-cache-miss {{$additional_groupby}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelColumnarPrefetchDuration = common.durationPanel(
    'Columnar Prefetch Duration',
    'tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket',
    selector=common.proxySelector,
  );

  local panelColumnarPrefetchCacheHitDuration = common.durationPanel(
    'Columnar Prefetch Cache Hit Duration',
    'tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket',
    selector=common.proxySelector,
  );

  local panelColumnarFetchSnapshotRetry = common.opsPanel(
    'Columnar Fetch Snapshot Retry',
    'tiflash_proxy_kv_engine_columnar_fetch_snapshot_retry_count',
    by=['$additional_groupby'],
    legend='retry {{$additional_groupby}}',
    selector=common.proxySelector,
    yRight='opm',
  );

  local panelColumnarFetchSnapshotDuration = common.durationPanel(
    'Columnar Fetch Snapshot Duration',
    'tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket',
    selector=common.proxySelector,
  );

  local panelColumnarMetaCache = graphPanel.new(
    title='Columnar Meta Cache',
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
      'sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_hit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by ($additional_groupby)',
      legendFormat='hit {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_miss{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by ($additional_groupby)',
      legendFormat='miss {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_parse{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by ($additional_groupby)',
      legendFormat='parse {{$additional_groupby}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelColumnarMetaCacheGauge = graphPanel.new(
    title='Columnar Meta Cache Gauge',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_proxy_kv_engine_columnar_meta_cache_entries{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
      legendFormat='entries-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_proxy_kv_engine_columnar_meta_cache_weighted_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
      legendFormat='weighted_size-{{instance}}',
    )
  )
  .addSeriesOverride({ alias: '/entries/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
  )
  .addYaxis(
    format='short',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelIaUsage, panelIaSegmentsMemoryWait]),
        common.band([panelIaSegmentRemoteReadCache, panelIaSegmentsRemoteReadDuration]),
        common.band([panelColumnarfileCache, panelColumnarPrefetchDuration, panelColumnarPrefetchCacheHitDuration]),
        common.band([panelColumnarFetchSnapshotRetry, panelColumnarFetchSnapshotDuration]),
        common.band([panelColumnarMetaCache, panelColumnarMetaCacheGauge])
      ],
    )
);

// --- Row: Storage ---
local rowStorage = (
  local rowObj = row.new(collapse=true, title='Storage');

  local panelWriteCommandOps = graphPanel.new(
    title='Write Command OPS',
    datasource=common.datasource,
    description='The total count of different kinds of commands received',
    fill=0,
    nullPointMode='null as zero',
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
      legendFormat='{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_DMWriteBlock{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
      legendFormat='write block',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/delete_range|ingest/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelWriteAmplification = graphPanel.new(
    title='Write Amplification',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
  )
  .addTarget(
    prometheus.target(
      'sum by (instance) ( tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} + tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} ) / sum by (instance) ( tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"} )',
      legendFormat='amp-total-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[5m]) )',
      legendFormat='amp-5min-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[10m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[10m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[10m]) )',
      legendFormat='amp-10min-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[30m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[30m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[30m]) )',
      legendFormat='amp-30min-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) )',
      legendFormat='fs-5min-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[5m]) )',
      legendFormat='write-5min-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addSeriesOverride({ alias: '/fs|write/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='short',
    min='0',
    max='20',
  )
  .addYaxis(
    format='binBps',
  );

  local panelSubtasksWriteThroughputBytes = graphPanel.new(
    title='SubTasks Write Throughput (bytes)',
    datasource=common.datasource,
    description='The throughput of (maybe foreground) tasks of storage in bytes',
    fill=0,
    nullPointMode='null',
    decimals=1,
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
      'sum(rate(tiflash_storage_subtask_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
      legendFormat='{{type}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/total/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='binBps',
    min='0',
  )
  .addYaxis(
    format='bytes',
    show=false,
  );

  local panelSubtasksWriteThroughputRows = graphPanel.new(
    title='SubTasks Write Throughput (rows)',
    datasource=common.datasource,
    description='The throughput of (maybe foreground) tasks of storage in rows',
    fill=0,
    nullPointMode='null',
    decimals=1,
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
      'sum(rate(tiflash_storage_subtask_throughput_rows{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
      legendFormat='{{type}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/total/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='none',
    min='0',
  )
  .addYaxis(
    format='bytes',
    show=false,
  );

  local panelSmallInternalTasksOps = common.opsPanel(
    'Small Internal Tasks OPS',
    'tiflash_storage_subtask_count',
    by=['type'],
    labels='type!~"(delta_merge|seg_merge|seg_split).*"',
    description="Total number of storage's internal sub tasks",
    yRight='opm',
  );

  local panelSmallInternalTasksDuration = common.durationPanel(
    'Small Internal Tasks Duration',
    'tiflash_storage_subtask_duration_seconds_bucket',
    selector=common.selector + ', type!~"(delta_merge|seg_merge|seg_split).*"',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
    description="Duration of storage's internal sub tasks",
  );

  local panelLargeInternalTasksOps = common.opsPanel(
    'Large Internal Tasks OPS',
    'tiflash_storage_subtask_count',
    by=['type'],
    labels='type=~"(delta_merge|seg_merge|seg_split).*"',
    description="Total number of storage's internal sub tasks",
    yRight='opm',
  );

  local panelLargeInternalTasksDuration = common.durationPanel(
    'Large Internal Tasks Duration',
    'tiflash_storage_subtask_duration_seconds_bucket',
    selector=common.selector + ', type=~"(delta_merge|seg_merge|seg_split).*"',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
    description="Duration of storage's internal sub tasks",
  );

  local panelCurrentDataManagementTasks = graphPanel.new(
    title='Current Data Management Tasks',
    datasource=common.datasource,
    description='The current processing number of  segments\' background management',
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
      'avg(tiflash_system_current_metric_DT_DeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='delta_merge-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'avg(tiflash_system_current_metric_DT_SegmentSplit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='seg_split-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'avg(tiflash_system_current_metric_DT_SegmentMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='seg_merge-{{instance}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
    min='0',
    decimals=0,
  )
  .addYaxis(
    format='none',
  );

  local panelOpenedFileCount = graphPanel.new(
    title='Opened File Count',
    datasource=common.datasource,
    description='The number of currently opened file descriptors.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
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
      'tiflash_proxy_process_open_fds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
      legendFormat='{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_OpenFileForWrite{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='W-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_OpenFileForRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='R-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_OpenFileForReadWrite{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='RW-{{instance}}',
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
    show=false,
  );

  local panelFileOpenOps = graphPanel.new(
    title='File Open OPS',
    datasource=common.datasource,
    description='The number of open file descriptors action.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
    fill=0,
    nullPointMode='null',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideEmpty=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
    legend_sideWidth=250,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_FileOpen{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='Open-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_FileOpenFailed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='OpenFail-{{instance}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='short',
    show=false,
  );

  local panelFsyncStatus = graphPanel.new(
    title='FSync Status',
    datasource=common.datasource,
    description='OPS and duration of fsync operations.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
    fill=0,
    nullPointMode='null as zero',
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
      'sum(rate(tiflash_system_profile_event_FileFSync{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='ops-fsync-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_system_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"fsync"}[$__rate_interval]))) by (le, instance) / 1000000000)',
      legendFormat='max-fsync-{{instance}}',
    )
  )
  .addSeriesOverride({ alias: '/max-fsync/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='s',
  );

  local panelDiskWriteOps = graphPanel.new(
    title='Disk Write OPS',
    datasource=common.datasource,
    description='The number of different kinds of read operations',
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMWriteIOCalls{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
      legendFormat='Page',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMWritePages{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='PageFile',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWrite{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='File Descriptor',
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

  local panelDiskReadOps = graphPanel.new(
    title='Disk Read OPS',
    datasource=common.datasource,
    description='The number of different kinds of read operations',
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMReadIOCalls{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='Page',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMReadPages{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='PageFile',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='File Descriptor',
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

  local panelWriteFlow = graphPanel.new(
    title='Write flow',
    datasource=common.datasource,
    description='The flow of different kinds of write operations',
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
      'sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='File Descriptor',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='Page',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMBackgroundWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='PageBackGround',
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

  local panelReadFlow = graphPanel.new(
    title='Read flow',
    datasource=common.datasource,
    description='The flow of different kinds of read operations',
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
      'sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='File Descriptor',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='Page',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='PageBackGround',
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

  local panelCompressionRatio = graphPanel.new(
    title='Compression Ratio',
    datasource=common.datasource,
    description='The compression ratio of different compression algorithm',
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_avg=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lz4_uncompressed_bytes"}[1m]))/sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lz4_compressed_bytes"}[1m]))',
      legendFormat='lz4',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lightweight_uncompressed_bytes"}[1m]))/sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lightweight_compressed_bytes"}[1m]))',
      legendFormat='lightweight',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
  )
  .addYaxis(
    format='short',
  );

  local panelCompressionAlgorithmCount = graphPanel.new(
    title='Compression Algorithm Count',
    datasource=common.datasource,
    description='The count of the compression algorithm used by each data part',
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_total=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_pack_compression_algorithm_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
      legendFormat='{{type}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
  )
  .addYaxis(
    format='short',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelWriteCommandOps, panelWriteAmplification]),
        common.band([panelSubtasksWriteThroughputBytes, panelSubtasksWriteThroughputRows]),
        common.band([panelSmallInternalTasksOps, panelSmallInternalTasksDuration], h=5),
        common.band([panelLargeInternalTasksOps, panelLargeInternalTasksDuration], h=5),
        common.band([panelCurrentDataManagementTasks]),
        common.band([panelOpenedFileCount, panelFileOpenOps, panelFsyncStatus]),
        common.band([panelDiskWriteOps, panelDiskReadOps]),
        common.band([panelWriteFlow, panelReadFlow]),
        common.band([panelCompressionRatio, panelCompressionAlgorithmCount])
      ],
    )
);

// --- Row: Storage Read Pool & Data Sharing ---
local rowStorageReadPoolDataSharing = (
  local rowObj = row.new(collapse=true, title='Storage Read Pool & Data Sharing');

  local panelReadTasksOps = common.opsPanel(
    'Read Tasks OPS',
    'tiflash_storage_read_tasks_count',
    by=['instance'],
    description='Total number of storage engine read tasks',
  );

  local panelReadSnapshots = graphPanel.new(
    title='Read Snapshots',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_SegmentReadTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='read_tasks-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_PSMVCCSnapshotsList{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='snapshot_list-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_PSMVCCNumSnapshots{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      format='heatmap',
      legendFormat='num_snapshot-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_SnapshotOfRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='read-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_SnapshotOfReadRaw{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='read_raw-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_SnapshotOfDeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='delta_merge-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_SnapshotOfDeltaCompact{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='delta_compact-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_SnapshotOfSegmentMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='seg_merge-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_SnapshotOfSegmentSplit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='seg_split-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_current_metric_DT_SnapshotOfPlaceIndex{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='place_index-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_asynchronous_metric_MaxDTDeltaOldestSnapshotLifetime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='max_snapshot_lifetime-{{instance}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/max_snapshot_lifetime/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='short',
    min='0',
  )
  .addYaxis(
    format='s',
    min='0',
  );

  local panelReadThreadInternalDuration = common.durationPanel(
    'Read Thread Internal Duration',
    'tiflash_read_thread_internal_us_bucket',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
    unit='µs',
  );

  local panelReadThreadScheduling = graphPanel.new(
    title='Read Thread Scheduling',
    datasource=common.datasource,
    description='The information of read thread scheduling.',
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_read_thread_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ru_exhausted|sche_active_segment_limit|sche_from_cache|sche_new_task|sche_no_pool|sche_no_ru|sche_no_segment|sche_no_slot|push_block_bytes"}[1m])) by (type)',
      legendFormat='{{type}}',
    )
  )
  .addSeriesOverride({ alias: '/push_block/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='binBps',
    min='0',
  );

  local panelDataSharing = common.opsHitRatioPanel(
    'Data Sharing',
    'tiflash_storage_read_thread_counter',
    [
      {
        metric: 'tiflash_storage_column_cache_packs',
        hitLabels: 'type=~"data_sharing_hit"',
        totalLabels: 'type=~"data_sharing_hit|data_sharing_miss"',
        legend: 'data_sharing_cache_hit_ratio',
        overrideAlias: '/cache_hit_ratio/',
      },
      {
        metric: 'tiflash_storage_column_cache_packs',
        hitLabels: 'type=~"extra_column_hit"',
        totalLabels: 'type=~"extra_column_hit|extra_column_miss"',
        legend: 'extra_column_cache_hit_ratio',
        overrideAlias: '/cache_hit_ratio/',
        hide: true,
      },
    ],
    by=['type'],
    labels='type=~"add_cache_total_bytes_limit"',
    legend='{{type}}',
    description='The information of data sharing cache hit ratio. Data sharing cache is purpose-built for OLAP workload that can reduce repeated data reads of concurrent table scanning.',
  );

  local panelSegmentMergedtask = graphPanel.new(
    title='Segment MergedTask',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum by (type,$additional_groupby) (tiflash_storage_read_thread_gauge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='{{type}} {{$additional_groupby}}',
    )
  )
  .addSeriesOverride({ alias: '/cache_hit_ratio/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='percentunit',
    min='0',
  );

  local panelSegmentMergedtaskDuration = common.durationPanel(
    'Segment MergedTask Duration',
    'tiflash_storage_read_thread_seconds_bucket',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
  );

  local panelVersionChain = common.durationPanel(
    'VersionChain',
    'tiflash_storage_version_chain_ms_bucket',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
    unit='ms',
  );

  local panelDeltaIndexError = graphPanel.new(
    title='DeltaIndexError',
    datasource=common.datasource,
    description='Errors of DeltaIndex',
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_max=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_DTDeltaIndexError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='DeltaIndexError-{{instance}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='cps',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
    show=false,
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelReadTasksOps, panelReadSnapshots]),
        common.band([panelReadThreadInternalDuration, panelReadThreadScheduling]),
        common.band([panelDataSharing, panelSegmentMergedtask, panelSegmentMergedtaskDuration]),
        common.band([panelVersionChain, panelDeltaIndexError])
      ],
    )
);

// --- Row: PageStorage ---
local rowPagestorage = (
  local rowObj = row.new(collapse=true, title='PageStorage');

  local panelPagestorageDiskUsage = graphPanel.new(
    title='PageStorage Disk Usage',
    datasource=common.datasource,
    description='The disk usage of PageStorage instances in each TiFlash node',
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
      'tiflash_system_asynchronous_metric_BlobDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='blob_disk_size-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_BlobValidBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='blob_valid_size-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum((tiflash_system_asynchronous_metric_BlobValidBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) / (tiflash_system_asynchronous_metric_BlobDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})) by (instance)',
      legendFormat='blob_valid_rate-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_asynchronous_metric_LogDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='log_size-{{instance}}',
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

  local panelPagestorageFileNum = graphPanel.new(
    title='PageStorage File Num',
    datasource=common.datasource,
    description='The number of files of PageStorage instances in each TiFlash node',
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
      'sum(tiflash_system_asynchronous_metric_BlobFileNums{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='blob_file-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_asynchronous_metric_LogNums{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='log_file-{{instance}}',
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

  local panelPagestorageWritebatchSize = common.heatmap(
    'PageStorage WriteBatch Size',
    'tiflash_storage_page_write_batch_size_bucket',
    yFormat='bytes',
    labels='type="v3"',
  );

  local panelPageWriteDuration = common.durationPanel(
    'Page write Duration',
    'tiflash_storage_page_write_duration_seconds_bucket',
    by=['type'],
    legend='{{type}}-%s {{$additional_groupby}}',
  );

  local panelPageGcTasksOpm = graphPanel.new(
    title='Page GC Tasks OPM',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null as zero',
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tiflash_storage_page_gc_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
      legendFormat='{{type}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='opm',
    min='0',
  )
  .addYaxis(
    format='short',
  );

  local panelPageGcDuration = common.durationPanel(
    'Page GC Duration',
    'tiflash_storage_page_gc_duration_seconds_bucket',
    by=['type'],
    legend='{{type}}-%s {{$additional_groupby}}',
  );

  local panelNumerOfPages = graphPanel.new(
    title='Numer of Pages',
    datasource=common.datasource,
    description='The number of pages of all TiFlash instance',
    fill=0,
    nullPointMode='null as zero',
    decimals=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_sort='current',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_asynchronous_metric_PagesInMem{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='num_pages-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_system_asynchronous_metric_VersionedEntries{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
      legendFormat='num_entries-{{instance}}',
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

  local panelPagestoragePendingWritersNum = graphPanel.new(
    title='PageStorage Pending Writers Num',
    datasource=common.datasource,
    description='The num of pending writers in PageStorage',
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
      'sum(tiflash_system_current_metric_PSPendingWriterNum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='size-{{instance}}',
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
    show=false,
  );

  local panelPagestorageStoredBytesByType = graphPanel.new(
    title='PageStorage stored bytes by type',
    datasource=common.datasource,
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
      'sum(tiflash_storage_page_data_by_types{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type)',
      legendFormat='{{type}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
  )
  .addYaxis(
    format='short',
    min='0',
  );

  local panelNumberOfTables = graphPanel.new(
    title='Number of Tables',
    datasource=common.datasource,
    description='The number of tables running under different mode in DeltaTree',
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
      'sum(tiflash_system_current_metric_StoragePoolV2Only{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='V2-{{instance}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_StoragePoolV3Only{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='V3-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_StoragePoolMixMode{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='Mix-{{instance}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_StoragePoolUniPS{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='UniPS-{{instance}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
  )
  .addYaxis(
    format='short',
  );

  local panelPsCommandOpsByInstance = graphPanel.new(
    title='PS Command OPS By Instance',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_page_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
      legendFormat='{{type}}-{{instance}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelPsApplyEditsOpsByInstance = graphPanel.new(
    title='PS Apply edits OPS By Instance',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_page_apply_edit_type{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
      legendFormat='{{type}}-{{instance}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelPagestorageDiskUsage, panelPagestorageFileNum]),
        common.band([panelPagestorageWritebatchSize, panelPageWriteDuration]),
        common.band([panelPageGcTasksOpm, panelPageGcDuration]),
        common.band([panelNumerOfPages, panelPagestoragePendingWritersNum]),
        common.band([panelPagestorageStoredBytesByType, panelNumberOfTables]),
        common.band([panelPsCommandOpsByInstance]),
        common.band([panelPsApplyEditsOpsByInstance])
      ],
    )
);

// --- Row: Rate Limiter ---
local rowRateLimiter = (
  local rowObj = row.new(collapse=true, title='Rate Limiter');

  local panelIOLimiterThroughput = graphPanel.new(
    title='I/O Limiter Throughput',
    datasource=common.datasource,
    description='The storage I/O limiter metrics.',
    fill=1,
    nullPointMode='null as zero',
    pointradius=2,
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
      'sum(rate(tiflash_storage_io_limiter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, instance)',
      legendFormat='{{type}}-{{instance}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='binBps',
    min='0',
    decimals=0,
  )
  .addYaxis(
    format='short',
  );

  local panelIOLimiterThreshold = graphPanel.new(
    title='I/O Limiter Threshold',
    datasource=common.datasource,
    description='Current limit bytes per second of Storage I/O limiter',
    fill=1,
    nullPointMode='null as zero',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_storage_io_limiter_curr{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type, instance)',
      legendFormat='{{type}}-{{instance}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
    decimals=0,
  )
  .addYaxis(
    format='short',
  );

  local panelIOLimiterCurrentPendingGauge = graphPanel.new(
    title='I/O Limiter Current Pending Gauge',
    datasource=common.datasource,
    description='I/O Limiter current pending gauge.',
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'avg(tiflash_system_current_metric_RateLimiterPendingWriteRequest{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='other-current-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'avg(tiflash_system_current_metric_IOLimiterPendingBgWriteReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='bgwrite-current-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'avg(tiflash_system_current_metric_IOLimiterPendingFgWriteReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='fgwrite-current-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'avg(tiflash_system_current_metric_IOLimiterPendingBgReadReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='bgread-current-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'avg(tiflash_system_current_metric_IOLimiterPendingFgReadReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='fgread-current-{{instance}}',
    )
  )
  .addSeriesOverride({ alias: '/pending/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='short',
    decimals=0,
  )
  .addYaxis(
    format='s',
  );

  local panelIOLimiterPendingOps = graphPanel.new(
    title='I/O Limiter Pending OPS',
    datasource=common.datasource,
    description='The storage I/O limiter metrics.',
    fill=1,
    nullPointMode='null as zero',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_io_limiter_pending_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, instance)',
      legendFormat='{{type}}-{{instance}}',
    )
  )
  .addSeriesOverride({ alias: '', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
    decimals=0,
  )
  .addYaxis(
    format='s',
  );

  local panelIOLimiterPendingDuration = common.durationPanel(
    'I/O Limiter Pending Duration',
    'tiflash_storage_io_limiter_pending_seconds_bucket',
    by=['type'],
    legend='{{type}}-pending-%s',
    description='I/O Limiter pending duration.',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelIOLimiterThroughput, panelIOLimiterThreshold]),
        common.band([panelIOLimiterCurrentPendingGauge, panelIOLimiterPendingOps, panelIOLimiterPendingDuration])
      ],
    )
);

// --- Row: Storage Write Stall ---
local rowStorageWriteStall = (
  local rowObj = row.new(collapse=true, title='Storage Write Stall');

  local panelWriteStallDuration = common.durationPanel(
    'Write Stall Duration',
    'tiflash_storage_write_stall_duration_seconds_bucket',
    by=['type', 'instance'],
    legend='%s-{{type}}-{{instance}}',
    description='The stall duration of write and delete range',
  )
  .addSeriesOverride({ alias: '99-delta_merge', yaxis: 2 });

  local panelWriteDeltaManagementThroughput = graphPanel.new(
    title='Write & Delta Management Throughput',
    datasource=common.datasource,
    description='The throughput of write and delta\'s background management',
    fill=0,
    nullPointMode='null',
    decimals=1,
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
      'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[1m]))',
      legendFormat='write+ingest',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"write|ingest"}[1m]))',
      legendFormat='ManageDelta',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='binBps',
    min='0',
  )
  .addYaxis(
    format='bytes',
    show=false,
  );

  local panelWriteDeltaManagementTotal = graphPanel.new(
    title='Write & Delta Management Total',
    datasource=common.datasource,
    description='The throughput of write and delta\'s background management',
    fill=0,
    nullPointMode='null',
    decimals=1,
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
      'sum(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"})',
      legendFormat='write+ingest',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"write|ingest"})',
      legendFormat='ManageDelta',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
  )
  .addYaxis(
    format='bytes',
    show=false,
  );

  local panelWriteThroughputByInstance = graphPanel.new(
    title='Write Throughput By Instance',
    datasource=common.datasource,
    description='The throughput of write by instance',
    fill=0,
    nullPointMode='null',
    decimals=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
    legend_sideWidth=250,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write"}[1m])) by (instance)',
      legendFormat='write-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ingest"}[1m])) by (instance)',
      legendFormat='ingest-{{instance}}',
    )
  )
  .addSeriesOverride({ alias: '/total/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='binBps',
    min='0',
  )
  .addYaxis(
    format='bytes',
    show=false,
  );

  local panelWriteCommandOpsByInstance = graphPanel.new(
    title='Write Command OPS By Instance',
    datasource=common.datasource,
    description='The total count of different kinds of commands received',
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_DMWriteBlock{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
      legendFormat='write block-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
      legendFormat='{{type}}-{{instance}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/delete_range|ingest/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelWriteStallDuration]),
        common.band([panelWriteDeltaManagementThroughput, panelWriteDeltaManagementTotal]),
        common.band([panelWriteThroughputByInstance]),
        common.band([panelWriteCommandOpsByInstance])
      ],
    )
);

// --- Row: Raft ---
local rowRaft = (
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

  common.buildRow(
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
    )
);

// --- Row: Raft Snapshot / IngestSST ---
local rowRaftSnapshotIngestsst = (
  local rowObj = row.new(collapse=true, title='Raft Snapshot / IngestSST');

  local panelHeavyRaftApplyDuration = common.durationPanel(
    'Heavy Raft Apply Duration',
    'tiflash_raft_command_duration_seconds_bucket',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
  );

  local panelApplyingSnapshotsCount = graphPanel.new(
    title='Applying snapshots Count',
    datasource=common.datasource,
    description='The number of currently applying snapshots.',
    fill=1,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_RaftNumSnapshotsPendingApply{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='Pending-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_RaftNumPrehandlingSubTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='PrehandleSubtasks-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_RaftNumParallelPrehandlingTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='ParallelTasks-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_RaftNumWaitedParallelPrehandlingTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='Pending-ParallelTasks-{{instance}}',
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

  local panelSnapshotUncommittedSizeHeatmap = common.heatmap(
    'Snapshot Uncommitted Size Heatmap',
    'tiflash_raft_write_flow_bytes_bucket',
    yFormat='bytes',
    labels='type=~"snapshot_uncommitted"',
    by=['le', 'type'],
  );

  local panelOngoingRaftSnapshot = graphPanel.new(
    title='Ongoing raft snapshot',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_raft_ongoing_snapshot_total_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
      legendFormat='{{le}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
  )
  .addYaxis(
    format='short',
  );

  local panelSnapshotSizeHeatmap = common.heatmap(
    'Snapshot Size Heatmap',
    'tiflash_raft_snapshot_total_bytes_bucket',
    yFormat='bytes',
    labels='type="approx_raft_snapshot"',
  );

  local panelSnapshotPredecodeDuration = common.heatmap(
    'Snapshot Predecode Duration',
    'tiflash_raft_command_duration_seconds_bucket',
    yFormat='s',
    labels='type="snapshot_predecode"',
    description='Duration of pre-decode when applying region snapshot',
  );

  local panelSnapshotPrehandleThroughputHeatmap = common.heatmap(
    'Snapshot Prehandle Throughput Heatmap',
    'tiflash_raft_command_throughput_seconds_bucket',
    yFormat='bytes',
    labels='type="prehandle_snapshot"',
  );

  local panelSnapshotFlushDuration = common.heatmap(
    'Snapshot Flush Duration',
    'tiflash_raft_command_duration_seconds_bucket',
    yFormat='s',
    labels='type="snapshot_flush"',
    description='Duration of pre-decode when applying region snapshot',
  );

  local panelIngestUncommittedSizeHeatmap = common.heatmap(
    'Ingest Uncommitted Size Heatmap',
    'tiflash_raft_write_flow_bytes_bucket',
    yFormat='bytes',
    labels='type=~"ingest_uncommitted"',
    by=['le', 'type'],
  );

  local panelSnapshotPredecodeSstToDtDuration = common.heatmap(
    'Snapshot Predecode SST to DT Duration',
    'tiflash_raft_command_duration_seconds_bucket',
    yFormat='s',
    labels='type="snapshot_predecode_sst2dt"',
    description='Duration of SST to DT in pre-decode when applying region snapshot',
  );

  local panelIngestSstDuration = common.heatmap(
    'Ingest SST Duration',
    'tiflash_raft_command_duration_seconds_bucket',
    yFormat='s',
    labels='type="ingest_sst"',
    description='Duration of ingesting SST',
  );
  common.buildRow(
      rowObj,
      [
        common.band([panelHeavyRaftApplyDuration]),
        common.band([panelApplyingSnapshotsCount]),
        common.band([panelSnapshotUncommittedSizeHeatmap, panelOngoingRaftSnapshot]),
        common.band([panelSnapshotSizeHeatmap, panelSnapshotPredecodeDuration]),
        common.band([panelSnapshotPrehandleThroughputHeatmap, panelSnapshotFlushDuration]),
        common.band([panelIngestUncommittedSizeHeatmap, panelSnapshotPredecodeSstToDtDuration]),
        common.band([{ panel: panelIngestSstDuration, w: 12 }])
      ],
    )
);

// --- Row: Rough Set Filter Rate Histogram ---
local rowRoughSetFilterRateHistogram = (
  local rowObj = row.new(collapse=true, title='Rough Set Filter Rate Histogram');

  local panelRoughSetFilterRate = graphPanel.new(
    title='Rough Set Filter Rate',
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
      'avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]) - rate(tiflash_system_profile_event_DMFileFilterAftRoughSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) / (rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (instance)',
      legendFormat='1min-{{instance}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) - rate(tiflash_system_profile_event_DMFileFilterAftRoughSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m])) / (rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]))) by (instance)',
      legendFormat='5min-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_DMFileFilterNoFilter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='No Filter-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='PK Filter-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_DMFileFilterAftRoughSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
      legendFormat='RS Filter-{{instance}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addSeriesOverride({ alias: '/^RS Filter/', yaxis: 2 })
  .addSeriesOverride({ alias: '/^PK/', yaxis: 2 })
  .addSeriesOverride({ alias: '/^No Filter/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='percentunit',
    min='0',
  )
  .addYaxis(
    format='short',
  );

  local panelRoughSetFilterRateHistogram = common.heatmap(
    'Rough Set Filter Rate Histogram',
    'tiflash_storage_rough_set_filter_rate_bucket',
    yFormat='percent',
  );
  common.buildRow(
      rowObj,
      [
        common.band([panelRoughSetFilterRate, panelRoughSetFilterRateHistogram])
      ],
    )
);

// --- Row: Disaggregated-Write ---
local rowDisaggregatedWrite = (
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

  common.buildRow(
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
    )
);

// --- Row: Disaggregated-Compute ---
local rowDisaggregatedCompute = (
  local rowObj = row.new(collapse=true, title='Disaggregated-Compute');

  local panelReadDurationBreakdown = common.durationPanel(
    'Read Duration Breakdown',
    'tiflash_disaggregated_breakdown_duration_seconds_bucket',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
  );

  local panelRemoteCacheOperations = common.opsHitRatioPanel(
    'Remote Cache Operations',
    'tiflash_storage_remote_cache',
    [
      {
        hitLabels: 'type=~"dtfile_hit"',
        totalLabels: 'type=~"dtfile_hit|dtfile_miss"',
        legend: 'dtfile_cache_hit_ratio',
      },
      {
        hitLabels: 'type=~"page_hit"',
        totalLabels: 'type=~"page_hit|page_miss"',
        legend: 'page_cache_hit_ratio',
      },
    ],
    by=['type', '$additional_groupby'],
    legend='{{type}} {{$additional_groupby}}',
    description='Remote Cache Operations',
  );

  local panelRemoteCacheFlow = graphPanel.new(
    title='Remote Cache Flow',
    datasource=common.datasource,
    description='Remote Cache Flow',
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_remote_cache_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='{{type}} {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='binBps',
    min='0',
  )
  .addYaxis(
    format='percentunit',
    min='0',
    show=false,
  );

  local panelRemoteCacheBgDownloadDuration = common.durationPanel(
    'Remote Cache BG Download Duration',
    'tiflash_storage_remote_cache_bg_download_stage_seconds_bucket',
    by=['stage', 'file_type'],
    legend='%s-{{stage}}-{{file_type}} {{$additional_groupby}}',
  );

  local panelRemoteCacheWaitOnDownloadingDuration = common.durationPanel(
    'Remote Cache Wait on Downloading Duration',
    'tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket',
    by=['result', 'file_type'],
    legend='%s-{{result}}-{{file_type}} {{$additional_groupby}}',
  );

  local panelRemoteCacheWaitOnDownloadingOps = graphPanel.new(
    title='Remote Cache Wait on Downloading OPS',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null as zero',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_remote_cache_wait_on_downloading_result{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (result, file_type , $additional_groupby)',
      legendFormat='{{result}}-{{file_type}} {{$additional_groupby}}',
    )
  )
  .addSeriesOverride({ alias: '', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
    decimals=0,
  )
  .addYaxis(
    format='s',
  );

  local panelRemoteCacheWaitOnDownloadingFlow = graphPanel.new(
    title='Remote Cache Wait on Downloading Flow',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_remote_cache_wait_on_downloading_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (result, file_type , $additional_groupby)',
      legendFormat='{{result}}-{{file_type}} {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='binBps',
    min='0',
  )
  .addYaxis(
    format='percentunit',
    min='0',
    show=false,
  );

  local panelRemoteCacheGauge = graphPanel.new(
    title='Remote Cache Gauge',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null as zero',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_storage_remote_cache_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type, instance)',
      legendFormat='{{type}}-{{instance}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
    decimals=0,
  )
  .addYaxis(
    format='short',
  );

  local panelRemoteCacheRejectDownloadTypeOps = graphPanel.new(
    title='Remote Cache Reject Download Type OPS',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null as zero',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_remote_cache_reject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (reason, file_type, $additional_groupby)',
      legendFormat='{{reason}}-{{file_type}} {{$additional_groupby}}',
    )
  )
  .addSeriesOverride({ alias: '', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
    decimals=0,
  )
  .addYaxis(
    format='s',
  );

  local panelRemoteCacheUsage = graphPanel.new(
    title='Remote Cache Usage',
    datasource=common.datasource,
    description='Remote Cache Usage',
    fill=0,
    nullPointMode='null as zero',
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
      'sum(tiflash_system_current_metric_DTFileCacheCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='DTFileCapacity-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_DTFileCacheUsed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='DTFileUsed-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_PageCacheCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='PageCapacity-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_PageCacheUsed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='PageUsed-{{instance}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
  )
  .addYaxis(
    format='percentunit',
    min='0',
    show=false,
  );

  local panelMemoryUsageOfStorageTasks = graphPanel.new(
    title='Memory Usage of Storage Tasks',
    datasource=common.datasource,
    description='Memory Usage of Storage Tasks',
    fill=0,
    nullPointMode='null as zero',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_max=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_MemoryTrackingQueryStorageTask{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='MemoryTrackingQueryStorageTask-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_MemoryTrackingFetchPages{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='MemoryTrackingFetchPages-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_DT_DeltaIndexCacheSize{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='DeltaIndexCacheSize-{{instance}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_system_current_metric_MemoryTrackingSharedColumnData{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
      legendFormat='SharedColumnData-{{instance}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
  )
  .addYaxis(
    format='percentunit',
    min='0',
    show=false,
  );

  local panelMvccIndexCache = common.opsHitRatioPanel(
    'MVCCIndexCache',
    'tiflash_storage_mvcc_index_cache',
    [
      {
        hitLabels: 'type=~"hit"',
        totalLabels: '',
        legend: 'hit_ratio-{{instance}}',
        overrideAlias: '/hit_ratio/',
        by: ['instance'],
      },
    ],
    by=['type', 'instance'],
    legend='{{type}}-{{instance}}',
    description='DeltaIndex cache of ReadNodes',
  );

  local panelPlaceindexTasksDuration = common.durationPanel(
    'PlaceIndex Tasks Duration',
    'tiflash_storage_subtask_duration_seconds_bucket',
    selector=common.selector + ', type="place_index_update"',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
    description="Duration of storage's internal sub tasks",
  );

  local panelPlaceindextaskReuseOps = graphPanel.new(
    title='PlaceIndexTask/Reuse OPS',
    datasource=common.datasource,
    description='Total number of storage\'s internal sub tasks',
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
      'sum(rate(tiflash_storage_place_index_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
      legendFormat='{{type}} {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_subtask_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"place_index_update"}[$__rate_interval])) by (type, $additional_groupby)',
      legendFormat='{{type}} {{$additional_groupby}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
    decimals=1,
  )
  .addYaxis(
    format='opm',
    min='0',
    show=false,
  );

  local panelPlaceindexUpdateRowsDeletes = graphPanel.new(
    title='PlaceIndex update rows/deletes',
    datasource=common.datasource,
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
      'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_place_index_stats_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, $additional_groupby) / 1000000000)',
      legendFormat='max {{$additional_groupby}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile(0.99, sum(rate(tiflash_storage_place_index_stats_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, $additional_groupby))',
      legendFormat='99-{{type}} {{$additional_groupby}}',
      intervalFactor=1,
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum by (type, $additional_groupby) (rate(tiflash_storage_place_index_stats_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) / sum by (type, $additional_groupby) (rate(tiflash_storage_place_index_stats_count_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
      legendFormat='avg-{{type}} {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
    decimals=2,
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelReadDurationBreakdown]),
        common.band([panelRemoteCacheOperations, panelRemoteCacheFlow]),
        common.band([panelRemoteCacheBgDownloadDuration, panelRemoteCacheWaitOnDownloadingDuration]),
        common.band([panelRemoteCacheWaitOnDownloadingOps, panelRemoteCacheWaitOnDownloadingFlow]),
        common.band([panelRemoteCacheGauge, panelRemoteCacheRejectDownloadTypeOps]),
        common.band([panelRemoteCacheUsage, panelMemoryUsageOfStorageTasks]),
        common.band([panelMvccIndexCache, panelPlaceindexTasksDuration]),
        common.band([panelPlaceindextaskReuseOps, panelPlaceindexUpdateRowsDeletes])
      ],
    )
);

// --- Row: S3 ---
local rowS3 = (
  local rowObj = row.new(collapse=true, title='S3');

  local panelS3Bytes = graphPanel.new(
    title='S3 Bytes',
    datasource=common.datasource,
    description='S3 read/write throughput',
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
      'sum(rate(tiflash_system_profile_event_S3WriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3WriteBytes {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3ReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3ReadBytes {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3WriteDMFileBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3WriteDMFileBytes {{$additional_groupby}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='binBps',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelS3Ops = graphPanel.new(
    title='S3 OPS',
    datasource=common.datasource,
    description='S3 OPS',
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
      'sum(rate(tiflash_system_profile_event_S3PutObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3PutObject {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3GetObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3GetObject {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3HeadObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3HeadObject {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3ListObjects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3ListObjects {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3DeleteObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3DeleteObject {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3CopyObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3CopyObject {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3CreateMultipartUpload{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3CreateMultipartUpload {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3UploadPart{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3UploadPart {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3CompleteMultipartUpload{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3CompleteMultipartUpload {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3PutDMFile{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3PutDMFile {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3IORead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IORead {{$additional_groupby}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3IOSeek{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IOSeek {{$additional_groupby}}',
      hide=true,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelS3RetryOps = graphPanel.new(
    title='S3 Retry OPS',
    datasource=common.datasource,
    description='S3 Retry OPS',
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
      'sum(rate(tiflash_system_profile_event_S3GetObjectRetry{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3GetObjectRetry {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3PutObjectRetry{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3PutObjectRetry {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3PutDMFileRetry{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3PutDMFileRetry {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3IOReadError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IOReadError {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3IOSeekError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IOSeekError {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3IOSeekBackward{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IOSeekBackward {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelS3RequestDuration = common.durationPanel(
    'S3 Request Duration',
    'tiflash_storage_s3_request_seconds_bucket',
    by=['type'],
    legend='{{type}}-%s {{$additional_groupby}}',
    description='S3 Request Duration',
  );

  local panelS3HttpOps = graphPanel.new(
    title='S3 HTTP OPS',
    datasource=common.datasource,
    description='S3 HTTP OPS',
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
      'sum(rate(tiflash_system_profile_event_S3ReadRequestsCount{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='read-count {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3WriteRequestsCount{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='write-count {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3ReadRequestsErrors{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='read-error {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3WriteRequestsErrors{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='write-error {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3ReadRequestsThrottling{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='read-throttling {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3WriteRequestsThrottling{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='write-throttling {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3ReadRequestsRedirects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='read-redirects {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3WriteRequestsRedirects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='write-redirects {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3ReadRequestsNotFound{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='read-notfound {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3WriteRequestsNotFound{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='write-notfound {{$additional_groupby}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelS3HttpRequestDuration = common.durationPanel(
    'S3 HTTP Request Duration',
    'tiflash_storage_s3_http_request_seconds_bucket',
    by=['type'],
    legend='{{type}}-%s {{$additional_groupby}}',
    description='S3 HTTP Request Duration',
  );

  local panelS3OnGoingInstances = graphPanel.new(
    title='S3 on-going instances',
    datasource=common.datasource,
    description='S3 HTTP OPS',
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
      'sum by (type, $additional_groupby) (tiflash_system_current_metric_S3Requests{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='S3Requests {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum by (type, $additional_groupby) (tiflash_system_current_metric_S3RandomAccessFile{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
      legendFormat='S3RandomAccessFile {{$additional_groupby}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='none',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  local panelS3randomaccessfileOps = graphPanel.new(
    title='S3RandomAccessFile OPS',
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
      'sum(rate(tiflash_system_profile_event_S3IOReadError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IOReadError {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3IOSeekError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IOSeekError {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3IOSeekBackward{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IOSeekBackward {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3IORead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IORead {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_system_profile_event_S3IOSeek{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
      legendFormat='S3IOSeek {{$additional_groupby}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='opm',
    min='0',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelS3Bytes, panelS3Ops]),
        common.band([panelS3RetryOps, panelS3RequestDuration]),
        common.band([panelS3HttpOps, panelS3HttpRequestDuration]),
        common.band([panelS3OnGoingInstances, panelS3randomaccessfileOps])
      ],
    )
);

// --- Row: Pipeline Model ---
local rowPipelineModel = (
  local rowObj = row.new(collapse=true, title='Pipeline Model');

  local panelTaskThreadPoolSize = graphPanel.new(
    title='Task Thread Pool Size',
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
      'max(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_task_thread_pool_size"}) by (instance, type)',
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

  local panelTaskCount = graphPanel.new(
    title='Task Count',
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
      'max(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_tasks_count"}) by (instance, type)',
      legendFormat='{{instance}}-{{type}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_tasks_count"}) by (type)',
      legendFormat='sum({{type}})',
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

  local panelTaskStatusChangeOps = common.opsPanel(
    'Task Status Change OPS',
    'tiflash_pipeline_task_change_to_status',
    by=['type'],
    yLeft='none',
    yRight='short',
  );

  local panelTaskDuration = common.durationPanel(
    'Task Duration',
    'tiflash_pipeline_task_duration_seconds_bucket',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
    extraTargets=[
      common.target(
        '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="cpu_execute"')
        + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="cpu_execute"') + ')',
        'avg-cpu_execute',
      ),
      common.target(
        '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="cpu_queue"')
        + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="cpu_queue"') + ')',
        'avg-cpu_queue',
      ),
      common.target(
        '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="io_execute"')
        + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="io_execute"') + ')',
        'avg-io_execute',
      ),
      common.target(
        '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="io_queue"')
        + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="io_queue"') + ')',
        'avg-io_queue',
      ),
      common.target(
        '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="await"')
        + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="await"') + ')',
        'avg-await',
      ),
    ],
  );

  local panelTaskMaxExecuteTimePerRound = graphPanel.new(
    title='Task Max Execute Time Per Round',
    datasource=common.datasource,
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
      'histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
      legendFormat='95-{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
      legendFormat='99-{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile(0.99, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
      legendFormat='999-{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type) / 1000000000)',
      legendFormat='100-{{type}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu"}[1m])) / sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu"}[1m]))',
      legendFormat='avg-cpu',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io"}[1m])) / sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io"}[1m]))',
      legendFormat='avg-io',
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

  local panelThreadsCpuOfCpuTaskThreadPool = common.cpuWithLimitPanel(
    'Threads CPU of CPU Task Thread Pool',
    'cpu_pool',
    legend='{{name}} {{instance}}',
  );

  local panelThreadsCpuOfIoTaskThreadPool = common.cpuWithLimitPanel(
    'Threads CPU of IO Task Thread Pool',
    'io_pool',
    legend='{{name}} {{instance}}',
  );

  local panelThreadsCpuOfWaitReactor = common.cpuWithLimitPanel(
    'Threads CPU of Wait Reactor',
    'WaitReactor',
    legend='{{name}} {{instance}}',
  );

  local panelWaitNotifyTaskDetails = graphPanel.new(
    title='Wait notify task details',
    datasource=common.datasource,
    description='wait notify task details',
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
      'max(tiflash_pipeline_wait_on_notify_tasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
      legendFormat='{{instance}}-{{type}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(tiflash_pipeline_wait_on_notify_tasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type)',
      legendFormat='sum({{type}})',
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

  common.buildRow(
      rowObj,
      [
        common.band([panelTaskThreadPoolSize, panelTaskCount]),
        common.band([panelTaskStatusChangeOps, panelTaskDuration]),
        common.band([panelTaskMaxExecuteTimePerRound, panelThreadsCpuOfCpuTaskThreadPool]),
        common.band([panelThreadsCpuOfIoTaskThreadPool, panelThreadsCpuOfWaitReactor]),
        common.band([{ panel: panelWaitNotifyTaskDetails, w: 12 }])
      ],
    )
);

// --- Row: TiFlash Resource Control ---
local rowTiflashResourceControl = (
  local rowObj = row.new(collapse=true, title='TiFlash Resource Control');

  local panelTiflashResourceGroup = graphPanel.new(
    title='TiFlash Resource Group',
    datasource=common.datasource,
    description='Metas of resource group',
    fill=1,
    nullPointMode='null',
    pointradius=2,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_avg=true,
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="remaining_tokens", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
      legendFormat='remaining_tokens-{{instance}}-{{resource_group}}',
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="avg_speed", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
      legendFormat='avg_speed-{{instance}}-{{resource_group}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="total_consumption", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
      legendFormat='total_consumption-{{instance}}-{{resource_group}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="bucket_fill_rate", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
      legendFormat='bucket_fill_rate-{{instance}}-{{resource_group}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="bucket_capacity", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
      legendFormat='bucket_capacity-{{instance}}-{{resource_group}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="request_gac_count", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
      legendFormat='request_gac_count-{{instance}}-{{resource_group}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="gac_req_ru_consumption_delta", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
      legendFormat='gac_req_ru_consumption_delta-{{instance}}-{{resource_group}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="compute_ru_consumption", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
      legendFormat='compute_ru_consumption-{{instance}}-{{resource_group}}',
      hide=true,
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="storage_ru_consumption", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
      legendFormat='storage_ru_consumption-{{instance}}-{{resource_group}}',
      hide=true,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
  )
  .addYaxis(
    format='short',
  );

  local panelRequestUnit = graphPanel.new(
    title='Request Unit',
    datasource=common.datasource,
    description='Request Unit for tidb-serverless charging',
    fill=0,
    nullPointMode='null as zero',
    decimals=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_sort='current',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_sync_replica_ru{instance=~"$tiflash_role"}[1m])) by (keyspace_id, $additional_groupby)',
      legendFormat='replica-sync-rate-{{keyspace_id}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tiflash_storage_sync_replica_ru{instance=~"$tiflash_role"}[24h])) by (keyspace_id, $additional_groupby)',
      legendFormat='replica-sync-sum-24h-{{keyspace_id}} {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_compute_request_unit{instance=~"$tiflash_role"}[1m])) by (cluster_id, $additional_groupby)',
      legendFormat='query-rate-{{cluster_id}} {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(increase(tiflash_compute_request_unit{instance=~"$tiflash_role"}[24h])) by (cluster_id, $additional_groupby)',
      legendFormat='query-sum-24h-{{cluster_id}} {{$additional_groupby}}',
    )
  )
  .addTarget(
    prometheus.target(
      'sum(rate(tiflash_storage_ru_read_bytes{instance=~"$tiflash_role"}[1m])) by (keyspace, resource_group, type, $additional_groupby) / (64 * 1024)',
      legendFormat='storage-{{keyspace}}_{{resource_group}}_{{type}} {{$additional_groupby}}',
    )
  )
  .addSeriesOverride({ alias: '/sum/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='cps',
    min='0',
  )
  .addYaxis(
    format='short',
    min='0',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelTiflashResourceGroup, panelRequestUnit])
      ],
    )
);

// --- Row: Status Server ---
local rowStatusServer = (
  local rowObj = row.new(collapse=true, title='Status Server');

  local panelStatusApiRequestDuration = common.durationPanel(
    'Status API Request Duration',
    'tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket',
    selector=common.proxySelector,
    by=['path'],
    legend='%s-{{path}} {{$additional_groupby}}',
  );

  local panelStatusApiRequestOpS = graphPanel.new(
    title='Status API Request (op/s)',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=true,
    legend_max=true,
    legend_hideEmpty=true,
    legend_hideZero=true,
  )
  .addTarget(
    prometheus.target(
      'sum(rate( tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_count {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"} [$__rate_interval] )) by (path, $additional_groupby)',
      legendFormat='{{path}} {{$additional_groupby}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='ops',
    min='0',
  )
  .addYaxis(
    format='short',
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelStatusApiRequestDuration, panelStatusApiRequestOpS])
      ],
    )
);

// --- Row: Vector Search ---
local rowVectorSearch = (
  local rowObj = row.new(collapse=true, title='Vector Search');

  local panelInMemoryVectorIndexInstances = graphPanel.new(
    title='In-Memory Vector Index Instances',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null as zero',
    decimals=0,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_max=true,
    legend_hideEmpty=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'sum by (type, instance) ( tiflash_vector_index_active_instances{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role" } )',
      legendFormat='{{instance}}-{{type}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='short',
    min='0',
    decimals=0,
  )
  .addYaxis(
    format='ops',
    min='0',
    show=false,
  );

  local panelVectorIndexEstimatedMemoryUsage = graphPanel.new(
    title='Vector Index Estimated Memory Usage',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null as zero',
    decimals=0,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_max=true,
    legend_hideEmpty=true,
    legend_hideZero=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'tiflash_vector_index_memory_usage{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role" }',
      legendFormat='{{instance}}-{{type}}',
      intervalFactor=1,
    )
  )
  .addTarget(
    prometheus.target(
      'tiflash_process_rss_by_type_bytes{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="file" }',
      legendFormat='{{instance}}-RssFile',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
    min='0',
    decimals=0,
  )
  .addYaxis(
    format='ops',
    min='0',
    show=false,
  );

  local panelP999VectorSearchDurationPerRequest = graphPanel.new(
    title='99.9% Vector Search Duration (Per Request)',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null as zero',
    decimals=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_max=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile( 0.999, sum(rate( tiflash_vector_index_duration_bucket{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!="build" } [$__rate_interval] )) by (le, type) )',
      legendFormat='{{type}}',
      intervalFactor=1,
    )
  )
  .addSeriesOverride({ alias: '/download/', yaxis: 2 })
  .resetYaxes()
  .addYaxis(
    format='s',
    min='0',
    decimals=1,
  )
  .addYaxis(
    format='s',
    min='0',
    decimals=1,
  );

  local panelP999VectorIndexBuildDurationPerDmfileColumn = graphPanel.new(
    title='99.9% Vector Index Build Duration (Per DMFile Column)',
    datasource=common.datasource,
    fill=0,
    nullPointMode='null as zero',
    decimals=1,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_max=true,
    legend_sort='max',
    legend_sortDesc=true,
  )
  .addTarget(
    prometheus.target(
      'histogram_quantile( 0.999, sum(rate( tiflash_vector_index_duration_bucket{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="build" } [$__rate_interval] )) by (le, type) )',
      legendFormat='{{type}}',
      intervalFactor=1,
    )
  )
  .resetYaxes()
  .addYaxis(
    format='s',
    min='0',
    decimals=1,
  )
  .addYaxis(
    format='s',
    min='0',
    show=false,
    decimals=1,
  );

  common.buildRow(
      rowObj,
      [
        common.band([panelInMemoryVectorIndexInstances, panelVectorIndexEstimatedMemoryUsage]),
        common.band([panelP999VectorSearchDurationPerRequest, panelP999VectorIndexBuildDurationPerDmfileColumn])
      ],
    )
);

local myNameFlag = 'DS_TEST-CLUSTER';

dashboard.new(
  title='Test-Cluster-TiFlash-Summary',
  uid='SVbh2xUWk',
  editable=true,
  graphTooltip='shared_crosshair',
  refresh='1m',
  time_from='now-1h',
  schemaVersion=27,
  style='dark',
)
.addInput(
  name=myNameFlag,
  label='Test-Cluster',
  type='datasource',
  pluginId='prometheus',
  pluginName='Prometheus',
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    hide='all',
    label='K8s-cluster',
    name='k8s_cluster',
    query='label_values(tiflash_system_profile_event_Query, k8s_cluster)',
    refresh='time',
    sort=1,
  )
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    hide='all',
    includeAll=false,
    label='tidb_cluster',
    multi=false,
    name='tidb_cluster',
    query='label_values(tiflash_system_profile_event_Query{k8s_cluster="$k8s_cluster"}, tidb_cluster)',
    refresh='time',
    sort=1,
  )
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    includeAll=true,
    label='Instance',
    multi=true,
    name='instance',
    query='label_values(tiflash_system_profile_event_Query{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
    refresh='load',
    sort=1,
  )
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    includeAll=true,
    label='Proxy Instance',
    multi=true,
    name='proxy_instance',
    query='label_values(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
    refresh='load',
    sort=1,
  )
)
.addTemplate(
  template.custom(
    name='additional_groupby',
    query='none,instance',
    current='none',
    label='additional_groupby',
  )
)
.addTemplate(
  template.custom(
    name='tiflash_role',
    query='.*,.*write-tiflash.*,.*compute-tiflash.*',
    current='.*',
    label='Role',
    valuelabels={
      '.*': 'All',
      '.*write-tiflash.*': 'Write',
      '.*compute-tiflash.*': 'Compute',
    },
  )
)
.addPanel(rowServer, gridPos=common.rowPos)
.addPanel(rowThreadsCpu, gridPos=common.rowPos)
.addPanel(rowThreads, gridPos=common.rowPos)
.addPanel(rowCoprocessor, gridPos=common.rowPos)
.addPanel(rowTaskScheduler, gridPos=common.rowPos)
.addPanel(rowDdl, gridPos=common.rowPos)
.addPanel(rowImbalanceReadWrite, gridPos=common.rowPos)
.addPanel(rowMemoryTrace, gridPos=common.rowPos)
.addPanel(rowColumnarStorage, gridPos=common.rowPos)
.addPanel(rowStorage, gridPos=common.rowPos)
.addPanel(rowStorageReadPoolDataSharing, gridPos=common.rowPos)
.addPanel(rowPagestorage, gridPos=common.rowPos)
.addPanel(rowRateLimiter, gridPos=common.rowPos)
.addPanel(rowStorageWriteStall, gridPos=common.rowPos)
.addPanel(rowRaft, gridPos=common.rowPos)
.addPanel(rowRaftSnapshotIngestsst, gridPos=common.rowPos)
.addPanel(rowRoughSetFilterRateHistogram, gridPos=common.rowPos)
.addPanel(rowDisaggregatedWrite, gridPos=common.rowPos)
.addPanel(rowDisaggregatedCompute, gridPos=common.rowPos)
.addPanel(rowS3, gridPos=common.rowPos)
.addPanel(rowPipelineModel, gridPos=common.rowPos)
.addPanel(rowTiflashResourceControl, gridPos=common.rowPos)
.addPanel(rowStatusServer, gridPos=common.rowPos)
.addPanel(rowVectorSearch, gridPos=common.rowPos)
