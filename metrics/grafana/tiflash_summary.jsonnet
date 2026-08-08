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

  local panelStoreSize = common.graph(
    'Store size',
    [
      common.target(
        common.expr.sum('tiflash_system_current_metric_StoreSizeUsed', common.selector, labels='type=~""', by=['instance']),
        '{{instance}}-local',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_StoreSizeUsedRemote', common.selector, by=['instance']),
        '{{instance}}-remote',
      ),
    ],
    description='The storage size per TiFlash instance.\n(Not including some disk usage of TiFlash-Proxy by now)',
    fill=5,
    linewidth=0,
    decimals=3,
    stack=true,
    legendMax=false,
    legendHideZero=true,
    legendHideEmpty=true,
    legendSort='current',
    yLeft='bytes',
  );

  local panelAvailableSize = common.graph(
    'Available size',
    [
      common.target(
        common.expr.sum('tiflash_system_current_metric_StoreSizeAvailable', common.selector, by=['instance']),
        '{{instance}}',
      ),
    ],
    description='The available capacity size per TiFlash instance',
    fill=5,
    linewidth=0,
    decimals=3,
    stack=true,
    legendMax=false,
    legendSort='current',
    yLeft='bytes',
  );

  local panelCapacitySize = common.graph(
    'Capacity size',
    [
      common.target(
        common.expr.sum('tiflash_system_current_metric_StoreSizeCapacity', common.selector, by=['instance']),
        '{{instance}}',
      ),
    ],
    description='The capacity size per TiFlash instance',
    fill=5,
    linewidth=0,
    decimals=3,
    stack=true,
    legendMax=false,
    legendSort='current',
    yLeft='bytes',
  );

  local panelUptime = common.graph(
    'Uptime',
    [
      prometheus.target(
        'tiflash_system_asynchronous_metric_Uptime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='{{instance}}',
      ),
    ],
    description='TiFlash uptime since last restart',
    fill=0,
    legendMax=false,
    legendSort='current',
    yLeft='dtdurations',
    yLeftMin=null,
  );

  local panelRegion = common.graph(
    'Region',
    [
      common.target(
        common.expr.sum('tiflash_proxy_tikv_raftstore_region_count', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="region", instance=~"$proxy_instance", instance=~"$tiflash_role"', by=['instance']),
        '{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_proxy_tikv_raftstore_hibernated_peer_state', common.proxySelector, by=['instance', 'state']),
        '{{instance}}-{{state}}',
        hide=true,
      ),
    ],
    description='The number of Regions on each TiFlash instance',
    fill=0,
    decimals=0,
    nullPointMode='null',
    legendHideZero=true,
    legendSort='current',
    yLeft='short',
    yLeftMin=null,
    yRight='short',
  );

  local panelCpuUsage = common.graph(
    'CPU Usage',
    [
      prometheus.target(
        'rate(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])',
        legendFormat='{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_LogicalCPUCores', common.selector, by=['instance']),
        'limit-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='TiFlash CPU usage calculated with process CPU running seconds.',
    fill=0,
    nullPointMode='null',
    seriesOverrides=[
      { alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 },
    ],
    yLeft='percentunit',
    yLeftDecimals=1,
    yRight='short',
  );

  local panelMemory = common.graph(
    'Memory',
    [
      common.target(
        common.expr.sum(
          'tiflash_proxy_process_resident_memory_bytes',
          common.proxySelector,
          labels='job=~".*tiflash"',
          by=['instance'],
        ),
        '{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_MemoryCapacity', common.selector, by=['instance']),
        'limit-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_jemalloc_retained', common.selector),
        'retained',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_jemalloc_mapped', common.selector),
        'mapped',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_jemalloc_resident', common.selector),
        'resident',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_jemalloc_allocated', common.selector),
        'allocated',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_jemalloc_active', common.selector),
        'active',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_jemalloc_metadata_thp', common.selector),
        'metadata_thp',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_jemalloc_metadata', common.selector),
        'metadata',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_mimalloc_current_rss', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$instance", instance=~"$tiflash_role"'),
        'mimalloc_rss',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_mimalloc_current_commit', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$instance", instance=~"$tiflash_role"'),
        'mimalloc_commit',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_mmap_alive', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$instance", instance=~"$tiflash_role"'),
        'mmap',
        hide=true,
        intervalFactor=1,
      ),
    ],
    description='The memory usage per TiFlash instance',
    fill=0,
    nullPointMode='null',
    seriesOverrides=[
      { alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 },
    ],
    yLeft='bytes',
    yRight='short',
  );

  local panelIoThroughput = common.graph(
    'IO Throughput',
    [
      common.target(
        common.expr.sumIrate(
          'tiflash_proxy_threads_io_bytes_total',
          common.proxySelector,
          labels='job=~".*tiflash"',
          by=['instance'],
        ),
        '{{instance}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    nullPointMode='null',
    yLeft='bytes',
    yLeftDecimals=0,
    yRight='short',
  );

  local panelRemoteStoreSummaryDisaggArch = common.graph(
    'Remote Store Summary (Disagg arch)',
    [
      common.target(
        common.expr.sum('tiflash_storage_s3_store_summary_bytes', common.selector, by=['instance', 'store_id', 'type']),
        'store-{{store_id}}-{{type}}',
      ),
    ],
    fill=0,
    decimals=1,
    legendMax=false,
    legendHideZero=true,
    legendSort='current',
    yLeft='bytes',
    yRight='short',
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

  local panelSstImportService = common.cpuWithLimitPanel(
    'SST Import Service',
    'sst_importer.*',
    description='Involved when importing data.',
    hideLimit=true,
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

  // PromQL string literals need \\d for regex \d → jsonnet '\\\\d'.
  local panelStorageBackgroundSmallTasks = common.cpuWithLimitPanel(
    'Storage Background (Small Tasks)',
    'bg_\\\\d+',
    legend='{{name}} {{instance}}',
  );

  local panelStorageBackgroundLargeTasks = common.cpuWithLimitPanel(
    'Storage Background (Large Tasks)',
    'bg_block_\\\\d+',
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

  local panelThreadsState = common.graph(
    'Threads state',
    [
      common.target(
        common.expr.sum('tiflash_proxy_threads_state', common.proxySelector, by=['instance', 'state']),
        '{{instance}}-{{state}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_proxy_threads_state', common.proxySelector, by=['instance']),
        '{{instance}}-total',
      ),
    ],
    decimals=1,
    nullPointMode='null',
    points=true,
    pointradius=2,
    legendSort='current',
    yLeft='none',
    yLeftMin=null,
    yRight='short',
  );

  local panelThreadsIo = common.graph(
    'Threads IO',
    [
      prometheus.target(
        'sum(rate(tiflash_proxy_threads_io_bytes_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[30s])) by (name, io, $additional_groupby) > 1024',
        legendFormat='{{name}}-{{io}} {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    decimals=1,
    nullPointMode='null',
    points=true,
    pointradius=2,
    yLeft='Bps',
    yLeftMin=null,
    yRight='short',
  );

  local panelThreadVoluntaryContextSwitches = common.graph(
    'Thread Voluntary Context Switches',
    [
      prometheus.target(
        'sum(rate(tiflash_proxy_thread_voluntary_context_switches{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[30s])) by (instance, name) > 200',
        legendFormat='{{instance}} - {{name}}',
        intervalFactor=1,
      ),
    ],
    decimals=1,
    nullPointMode='null',
    points=true,
    pointradius=2,
    yLeft='none',
    yLeftMin=null,
    yRight='short',
  );

  local panelThreadNonvoluntaryContextSwitches = common.graph(
    'Thread Nonvoluntary Context Switches',
    [
      prometheus.target(
        'sum(rate(tiflash_proxy_thread_nonvoluntary_context_switches{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[30s])) by (instance, name) > 50',
        legendFormat='{{instance}} - {{name}}',
        intervalFactor=1,
      ),
    ],
    decimals=1,
    nullPointMode='null',
    points=true,
    pointradius=2,
    yLeft='none',
    yLeftMin=null,
    yRight='short',
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

  local panelResponseBytesSeconds = common.graph(
    'Response Bytes/Seconds',
    [
      common.target(
        common.expr.sumRate('tiflash_coprocessor_response_bytes', common.selector, by=['type'], range='1m'),
        '{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    yLeft='bytes',
    yRight='short',
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
    show=false,
  );

  local panelExchangeBytesSeconds = common.graph(
    'Exchange Bytes/Seconds',
    [
      common.target(
        common.expr.sumRate('tiflash_exchange_data_bytes', common.selector, by=['type'], range='1m'),
        '{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    yLeft='bytes',
    yRight='short',
  );

  local panelThreadsOfRpc = common.graph(
    'Threads of Rpc',
    [
      prometheus.target(
        'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"rpc.*", type!~".*max"}) by (instance, type)',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='short',
  );

  local panelHandlingRequestNumber = common.graph(
    'Handling Request Number',
    [
      common.target(
        common.expr.sum('tiflash_coprocessor_handling_request_count', common.selector, by=['type']),
        '{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='none',
  );

  local panelThreads = common.graph(
    'Threads',
    [
      prometheus.target(
        'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~".*max", type!~"rpc.*"}) by (instance, type)',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='short',
  );

  local panelMaxThreadsOfRpc = common.graph(
    'Max Threads of Rpc',
    [
      prometheus.target(
        'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"rpc.*", type=~".*max"}) by (instance, type)',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='short',
  );

  local panelMppQueryCount = common.graph(
    'MPP Query count',
    [
      prometheus.target(
        'max(tiflash_mpp_task_manager{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
    ],
    description='The MPP query count in TiFlash',
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='short',
  );

  local panelMaxThreads = common.graph(
    'Max Threads',
    [
      prometheus.target(
        'max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*max", type!~"rpc.*"}) by (instance, type)',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='short',
  );

  local panelTimeOfTheLongestLiveMppTask = common.graph(
    'Time of the Longest Live MPP Task',
    [
      prometheus.target(
        'tiflash_mpp_task_monitor{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='{{instance}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='s',
    yRight='short',
  );

  local panelDataSizeInSendAndReceiveQueue = common.graph(
    'Data size in send and receive queue',
    [
      prometheus.target(
        'tiflash_exchange_queueing_data_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    yLeft='bytes',
    yRight='short',
  );

  local panelNetworkTransmission = common.graph(
    'Network Transmission',
    [
      common.target(
        common.expr.sumRate('tiflash_network_transmission_bytes', common.selector, by=['type'], range='1m'),
        '{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='bytes',
    yRight='short',
  );

  local panelEstablishCalldataDetails = common.graph(
    'Establish calldata details',
    [
      prometheus.target(
        'max(tiflash_establish_calldata_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type != "new_request_calldata"}) by (instance, type)',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
    ],
    description='The establish calldata details',
    fill=0,
    yLeft='none',
    yRight='short',
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
    show=false,
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
    show=false,
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
    show=false,
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
    show=false,
  );

  local panelHardLimitExceededCount = common.graph(
    'Hard Limit Exceeded Count',
    [
      prometheus.target(
        'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="hard_limit_exceeded_count"}) by (instance, type, resource_group)',
        legendFormat='{{instance}}-{{resource_group}}',
      ),
    ],
    description='the usage of estimated threads exceeded the hard limit where errors occur.',
    fill=0,
    pointradius=1,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yLeftMin=null,
    yRight='short',
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
    show=false,
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
    show=false,
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

  local panelCpuUsageIrate = common.graph(
    'CPU Usage (irate)',
    [
      prometheus.target(
        'irate(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$tiflash_role"}[1m])',
        legendFormat='{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_LogicalCPUCores', common.selector, by=['instance']),
        'limit-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='TiFlash CPU usage calculated with process CPU running seconds.',
    fill=0,
    nullPointMode='null',
    sideWidth=250,
    seriesOverrides=[
      { alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 },
    ],
    yLeft='percentunit',
    yLeftDecimals=1,
    yRight='short',
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

  local panelReadThroughputByInstance = common.graph(
    'Read Throughput by instance',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes', common.selector, by=['instance'], range='1m'),
        'File Descriptor-{{instance}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMReadBytes', common.selector, by=['instance'], range='1m'),
        'Page-{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMBackgroundReadBytes', common.selector, by=['instance'], range='1m'),
        'PageBackGround-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='The flow of different kinds of read operations',
    decimals=1,
    nullPointMode='null',
    legendHideZero=true,
    legendSort='current',
    yLeft='binBps',
    yRight='short',
    yRightMin='0',
  );

  local panelWriteCommandOpsByInstance = common.graph(
    'Write Command OPS By Instance',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_DMWriteBlock', common.selector, by=['instance', 'type'], range='1m'),
        'write block-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
        legendFormat='{{type}}-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='The total count of different kinds of commands received',
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/delete_range|ingest/', yaxis: 2 },
    ],
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
    yRightShow=true,
  );

  local panelWriteThroughputByInstance = common.graph(
    'Write Throughput By Instance',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_throughput_bytes', common.selector, labels='type=~"write"', by=['instance'], range='1m'),
        'write-{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_storage_throughput_bytes', common.selector, labels='type=~"ingest"', by=['instance'], range='1m'),
        'ingest-{{instance}}',
      ),
    ],
    description='The throughput of write by instance',
    fill=0,
    decimals=1,
    nullPointMode='null',
    sideWidth=250,
    seriesOverrides=[
      { alias: '/total/', yaxis: 2 },
    ],
    yLeft='binBps',
    yRight='bytes',
    yRightShow=true,
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

  local panelNumberOfKeyspaces = common.graph(
    'Number of Keyspaces',
    [
      prometheus.target(
        'tiflash_system_current_metric_NumKeyspace{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='keyspace-{{instance}}',
        intervalFactor=1,
      ),
    ],
    nullPointMode='null',
    pointradius=2,
    legendSort=null,
    legendSortDesc=null,
    yLeft='short',
    yLeftMin=null,
    yRight='s',
  );

  local panelNumberOfPhysicalTables = common.graph(
    'Number of Physical Tables',
    [
      prometheus.target(
        'tiflash_system_current_metric_DT_NumStorageDeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='tables-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_system_current_metric_NumIStorage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='tables-all-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
    ],
    nullPointMode='null',
    pointradius=2,
    legendSort=null,
    legendSortDesc=null,
    yLeft='short',
    yLeftMin=null,
    yRight='s',
  );

  local panelNumberOfSegments = common.graph(
    'Number of Segments',
    [
      prometheus.target(
        'tiflash_system_current_metric_DT_NumSegment{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='segments-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_system_current_metric_DT_NumMemTable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='mem_table-{{instance}}',
        intervalFactor=1,
      ),
    ],
    nullPointMode='null',
    pointradius=2,
    legendSort=null,
    legendSortDesc=null,
    yLeft='short',
    yLeftMin=null,
    yRight='s',
  );

  local panelBytesOfMemtables = common.graph(
    'Bytes of MemTables',
    [
      prometheus.target(
        'tiflash_system_current_metric_DT_BytesMemTable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='bytes-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_system_current_metric_DT_BytesMemTableAllocated{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='bytes-allocated-{{instance}}',
        intervalFactor=1,
      ),
    ],
    nullPointMode='null',
    pointradius=2,
    legendSort=null,
    legendSortDesc=null,
    yLeft='bytes',
    yLeftMin=null,
    yRight='s',
  );

  local panelMarkCacheAndMinmaxIndexCacheMemoryUsage = common.graph(
    'Mark Cache and Minmax Index Cache Memory Usage',
    [
      prometheus.target(
        'tiflash_system_asynchronous_metric_MarkCacheBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='mark_cache_{{instance}}',
      ),
      prometheus.target(
        'tiflash_system_asynchronous_metric_MinMaxIndexFiles{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='minmax_index_cache_{{instance}}',
      ),
      prometheus.target(
        'tiflash_system_asynchronous_metric_RNMVCCIndexCacheBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='rn_mvcc_index_cache_{{instance}}',
      ),
    ],
    description='The memory usage of mark cache and minmax index cache',
    fill=0,
    nullPointMode='null',
    sideWidth=250,
    seriesOverrides=[
      { alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 },
    ],
    yLeft='bytes',
    yRight='short',
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

  local panelSchemaOfColumnFile = common.graph(
    'Schema of Column File',
    [
      prometheus.target(
        'max(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"current_size"}) by (instance)',
        legendFormat='current_size-{{instance}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_shared_block_schemas', common.selector, labels='type=~"hit_count"', by=['instance'], range='1m'),
        'hit_count_ops-{{instance}}',
      ),
      prometheus.target(
        'max(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"still_used_when_evict"}) by (instance)',
        legendFormat='still_used_when_evict-{{instance}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_shared_block_schemas', common.selector, labels='type=~"miss_count"', by=['instance'], range='1m'),
        'miss_count_ops-{{instance}}',
      ),
    ],
    description='Information about schema of column file, to learn the memory usage of schema',
    nullPointMode='null',
    pointradius=2,
    legendMax=false,
    legendSort='current',
    yLeft='short',
    yLeftMin=null,
    yRight='short',
  );

  local panelReadSnapshots = common.graph(
    'Read Snapshots',
    [
      prometheus.target(
        'tiflash_system_current_metric_DT_SegmentReadTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='read_tasks-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_system_asynchronous_metric_MaxDTDeltaOldestSnapshotLifetime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='max_snapshot_lifetime-{{instance}}',
        intervalFactor=1,
      ),
    ],
    nullPointMode='null',
    pointradius=2,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/max_snapshot_lifetime/', yaxis: 2 },
    ],
    yLeft='short',
    yRight='s',
    yRightMin='0',
    yRightShow=true,
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
    show=false,
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
    show=false,
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
    show=false,
  );

  local panelKvstoreMemory = graphPanel.new(
    title='KVStore memory',
    datasource=common.datasource,
    fill=1,
    nullPointMode='null',
    pointradius=2,
  )
  .addTarget(
    common.target(
      common.expr.sum('tiflash_system_current_metric_MemoryTrackingKVStore', common.selector, by=['instance']),
      '{{instance}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
  )
  .addYaxis(
    format='short',
    show=false,
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

  local panelIaUsage = common.graph(
    'IA usage',
    [
      prometheus.target(
        'tiflash_proxy_kv_engine_ia_main_queue_capacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
        legendFormat='capacity-main-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_proxy_kv_engine_ia_small_queue_capacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
        legendFormat='capacity-small-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_proxy_kv_engine_ia_manager_segments_memory_capacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
        legendFormat='capacity-segments-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_proxy_kv_engine_ia_manager_segments_memory_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
        legendFormat='segments-mem-size-{{instance}}',
      ),
      prometheus.target(
        'tiflash_proxy_kv_engine_ia_manager_segments_disk_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
        legendFormat='segments-disk-size-{{instance}}',
      ),
    ],
    fill=0,
    nullPointMode='null',
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 },
    ],
    yLeft='bytes',
    yRight='short',
  );

  local panelIaSegmentsMemoryWait = common.durationPanel(
    'IA Segments Memory Wait',
    'tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket',
    selector=common.proxySelector,
  );

  local panelIaSegmentRemoteReadCache = common.graph(
    'IA Segment Remote Read Cache',
    [
      common.target(
        common.expr.sumRate('tiflash_proxy_kv_engine_ia_remote_read_segment_cache_hit', common.proxySelector, by=['$additional_groupby'], range='1m'),
        'cache-hit {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_proxy_kv_engine_ia_remote_read_segment_cache_miss', common.proxySelector, by=['$additional_groupby'], range='1m'),
        'cache-miss {{$additional_groupby}}',
      ),
    ],
    fill=0,
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
  );

  local panelIaSegmentsRemoteReadDuration = common.durationPanel(
    'IA Segments Remote Read Duration',
    'tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket',
    selector=common.proxySelector,
  );

  local panelColumnarfileCache = common.graph(
    'ColumnarFile Cache',
    [
      common.target(
        common.expr.sumRate('tiflash_proxy_kv_engine_columnar_file_cache_hit', common.proxySelector, by=['$additional_groupby'], range='1m'),
        'file-cache-hit {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_proxy_kv_engine_columnar_file_cache_miss', common.proxySelector, by=['$additional_groupby'], range='1m'),
        'file-cache-miss {{$additional_groupby}}',
      ),
    ],
    fill=0,
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
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

  local panelColumnarMetaCache = common.graph(
    'Columnar Meta Cache',
    [
      common.target(
        common.expr.sumRate('tiflash_proxy_kv_engine_columnar_meta_cache_hit', common.proxySelector, by=['$additional_groupby'], range='1m'),
        'hit {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_proxy_kv_engine_columnar_meta_cache_miss', common.proxySelector, by=['$additional_groupby'], range='1m'),
        'miss {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_proxy_kv_engine_columnar_meta_cache_parse', common.proxySelector, by=['$additional_groupby'], range='1m'),
        'parse {{$additional_groupby}}',
      ),
    ],
    fill=0,
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
  );

  local panelColumnarMetaCacheGauge = common.graph(
    'Columnar Meta Cache Gauge',
    [
      prometheus.target(
        'tiflash_proxy_kv_engine_columnar_meta_cache_entries{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
        legendFormat='entries-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_proxy_kv_engine_columnar_meta_cache_weighted_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
        legendFormat='weighted_size-{{instance}}',
      ),
    ],
    fill=0,
    nullPointMode='null',
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/entries/', yaxis: 2 },
    ],
    yLeft='bytes',
    yRight='short',
    yRightShow=true,
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
    common.target(
      common.expr.sumRate('tiflash_system_profile_event_DMWriteBlock', common.selector, by=['type'], range='1m'),
      'write block',
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

  local panelWriteAmplification = common.graph(
    'Write Amplification',
    [
      prometheus.target(
        'sum by (instance) ( tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} + tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} ) / sum by (instance) ( tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"} )',
        legendFormat='amp-total-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[5m]) )',
        legendFormat='amp-5min-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[10m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[10m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[10m]) )',
        legendFormat='amp-10min-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[30m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[30m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[30m]) )',
        legendFormat='amp-30min-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) )',
        legendFormat='fs-5min-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[5m]) )',
        legendFormat='write-5min-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
    ],
    fill=0,
    nullPointMode='null',
    legendMax=false,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/fs|write/', yaxis: 2 },
    ],
    yLeft='short',
    yLeftMax='20',
    yRight='binBps',
    yRightShow=true,
  );

  local panelSubtasksWriteThroughputBytes = common.graph(
    'SubTasks Write Throughput (bytes)',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_subtask_throughput_bytes', common.selector, by=['type'], range='1m'),
        '{{type}}',
        intervalFactor=1,
      ),
    ],
    description='The throughput of (maybe foreground) tasks of storage in bytes',
    fill=0,
    decimals=1,
    nullPointMode='null',
    sideWidth=250,
    seriesOverrides=[
      { alias: '/total/', yaxis: 2 },
    ],
    yLeft='binBps',
    yRight='bytes',
    yRightShow=true,
  );

  local panelSubtasksWriteThroughputRows = common.graph(
    'SubTasks Write Throughput (rows)',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_subtask_throughput_rows', common.selector, by=['type'], range='1m'),
        '{{type}}',
        intervalFactor=1,
      ),
    ],
    description='The throughput of (maybe foreground) tasks of storage in rows',
    fill=0,
    decimals=1,
    nullPointMode='null',
    sideWidth=250,
    seriesOverrides=[
      { alias: '/total/', yaxis: 2 },
    ],
    yLeft='none',
    yRight='bytes',
    yRightShow=true,
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

  local panelCurrentDataManagementTasks = common.graph(
    'Current Data Management Tasks',
    [
      prometheus.target(
        'avg(tiflash_system_current_metric_DT_DeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
        legendFormat='delta_merge-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'avg(tiflash_system_current_metric_DT_SegmentSplit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
        legendFormat='seg_split-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'avg(tiflash_system_current_metric_DT_SegmentMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
        legendFormat='seg_merge-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='The current processing number of  segments\' background management',
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='short',
    yLeftDecimals=0,
    yRight='none',
  );

  local panelOpenedFileCount = common.graph(
    'Opened File Count',
    [
      prometheus.target(
        'tiflash_proxy_process_open_fds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
        legendFormat='{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_OpenFileForWrite', common.selector, by=['instance']),
        'W-{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_OpenFileForRead', common.selector, by=['instance']),
        'R-{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_OpenFileForReadWrite', common.selector, by=['instance']),
        'RW-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='The number of currently opened file descriptors.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
    fill=0,
    nullPointMode='null',
    sideWidth=250,
    yLeft='none',
    yRight='short',
  );

  local panelFileOpenOps = common.graph(
    'File Open OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_FileOpen', common.selector, by=['instance'], range='1m'),
        'Open-{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_FileOpenFailed', common.selector, by=['instance'], range='1m'),
        'OpenFail-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='The number of open file descriptors action.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
    fill=0,
    nullPointMode='null',
    legendHideZero=true,
    legendHideEmpty=true,
    sideWidth=250,
    yLeft='ops',
    yRight='short',
  );

  local panelFsyncStatus = common.graph(
    'FSync Status',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_FileFSync', common.selector, by=['instance'], range='1m'),
        'ops-fsync-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_system_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"fsync"}[$__rate_interval]))) by (le, instance) / 1000000000)',
        legendFormat='max-fsync-{{instance}}',
      ),
    ],
    description='OPS and duration of fsync operations.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
    fill=0,
    sideWidth=250,
    seriesOverrides=[
      { alias: '/max-fsync/', yaxis: 2 },
    ],
    yLeft='ops',
    yRight='s',
    yRightShow=true,
  );

  local panelDiskWriteOps = common.graph(
    'Disk Write OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMWriteIOCalls', common.selector, by=['type'], range='1m'),
        'Page',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMWritePages', common.selector, range='1m'),
        'PageFile',
        hide=true,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_WriteBufferFromFileDescriptorWrite', common.selector, range='1m'),
        'File Descriptor',
        intervalFactor=1,
      ),
    ],
    description='The number of different kinds of read operations',
    fill=0,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    yLeft='ops',
    yRight='none',
  );

  local panelDiskReadOps = common.graph(
    'Disk Read OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMReadIOCalls', common.selector, range='1m'),
        'Page',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMReadPages', common.selector, range='1m'),
        'PageFile',
        hide=true,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_ReadBufferFromFileDescriptorRead', common.selector, range='1m'),
        'File Descriptor',
        intervalFactor=1,
      ),
    ],
    description='The number of different kinds of read operations',
    fill=0,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    yLeft='ops',
    yRight='none',
  );

  local panelWriteFlow = common.graph(
    'Write flow',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes', common.selector, range='1m'),
        'File Descriptor',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMWriteBytes', common.selector, range='1m'),
        'Page',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMBackgroundWriteBytes', common.selector, range='1m'),
        'PageBackGround',
        intervalFactor=1,
      ),
    ],
    description='The flow of different kinds of write operations',
    decimals=1,
    nullPointMode='null',
    legendHideZero=true,
    legendSort='current',
    yLeft='binBps',
    yRight='short',
    yRightMin='0',
  );

  local panelReadFlow = common.graph(
    'Read flow',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes', common.selector, range='1m'),
        'File Descriptor',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMReadBytes', common.selector, range='1m'),
        'Page',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_PSMBackgroundReadBytes', common.selector, range='1m'),
        'PageBackGround',
        intervalFactor=1,
      ),
    ],
    description='The flow of different kinds of read operations',
    decimals=1,
    nullPointMode='null',
    legendHideZero=true,
    legendSort='current',
    yLeft='binBps',
    yRight='short',
    yRightMin='0',
  );

  local panelCompressionRatio = common.graph(
    'Compression Ratio',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_pack_compression_bytes', common.selector, labels='type=~"lz4_uncompressed_bytes"}[1m]))/sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lz4_compressed_bytes"', range='1m'),
        'lz4',
      ),
      common.target(
        common.expr.sumRate('tiflash_storage_pack_compression_bytes', common.selector, labels='type=~"lightweight_uncompressed_bytes"}[1m]))/sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lightweight_compressed_bytes"', range='1m'),
        'lightweight',
      ),
    ],
    description='The compression ratio of different compression algorithm',
    nullPointMode='null',
    pointradius=2,
    legendMax=false,
    legendAvg=true,
    legendSort=null,
    legendSortDesc=null,
    yLeft='short',
    yLeftMin=null,
    yRight='short',
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
    common.target(
      common.expr.sumRate('tiflash_storage_pack_compression_algorithm_count', common.selector, by=['type'], range='1m'),
      '{{type}}',
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

  local panelReadSnapshots = common.graph(
    'Read Snapshots',
    [
      prometheus.target(
        'tiflash_system_current_metric_DT_SegmentReadTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='read_tasks-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_system_current_metric_PSMVCCSnapshotsList{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='snapshot_list-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'tiflash_system_current_metric_PSMVCCNumSnapshots{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        format='heatmap',
        legendFormat='num_snapshot-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'tiflash_system_current_metric_DT_SnapshotOfRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='read-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'tiflash_system_current_metric_DT_SnapshotOfReadRaw{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='read_raw-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'tiflash_system_current_metric_DT_SnapshotOfDeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='delta_merge-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'tiflash_system_current_metric_DT_SnapshotOfDeltaCompact{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='delta_compact-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'tiflash_system_current_metric_DT_SnapshotOfSegmentMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='seg_merge-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'tiflash_system_current_metric_DT_SnapshotOfSegmentSplit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='seg_split-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'tiflash_system_current_metric_DT_SnapshotOfPlaceIndex{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='place_index-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'tiflash_system_asynchronous_metric_MaxDTDeltaOldestSnapshotLifetime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='max_snapshot_lifetime-{{instance}}',
        intervalFactor=1,
      ),
    ],
    nullPointMode='null',
    pointradius=2,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/max_snapshot_lifetime/', yaxis: 2 },
    ],
    yLeft='short',
    yRight='s',
    yRightMin='0',
    yRightShow=true,
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
    common.target(
      common.expr.sumRate('tiflash_storage_read_thread_counter', common.selector, labels='type=~"ru_exhausted|sche_active_segment_limit|sche_from_cache|sche_new_task|sche_no_pool|sche_no_ru|sche_no_segment|sche_no_slot|push_block_bytes"', by=['type'], range='1m'),
      '{{type}}',
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

  local panelSegmentMergedtask = common.graph(
    'Segment MergedTask',
    [
      common.target(
        common.expr.sum('tiflash_storage_read_thread_gauge', common.selector, by=['type', '$additional_groupby']),
        '{{type}} {{$additional_groupby}}',
      ),
    ],
    fill=0,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/cache_hit_ratio/', yaxis: 2 },
    ],
    yLeft='ops',
    yRight='percentunit',
    yRightMin='0',
    yRightShow=true,
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

  local panelDeltaIndexError = common.graph(
    'DeltaIndexError',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_DTDeltaIndexError', common.selector, by=['instance'], range='1m'),
        'DeltaIndexError-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='Errors of DeltaIndex',
    fill=0,
    legendCurrent=false,
    legendSort=null,
    legendSortDesc=null,
    yLeft='cps',
    yRight='opm',
    yRightMin='0',
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

  local panelPagestorageDiskUsage = common.graph(
    'PageStorage Disk Usage',
    [
      prometheus.target(
        'tiflash_system_asynchronous_metric_BlobDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='blob_disk_size-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_BlobValidBytes', common.selector, by=['instance']),
        'blob_valid_size-{{instance}}',
      ),
      prometheus.target(
        'sum((tiflash_system_asynchronous_metric_BlobValidBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) / (tiflash_system_asynchronous_metric_BlobDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})) by (instance)',
        legendFormat='blob_valid_rate-{{instance}}',
      ),
      prometheus.target(
        'tiflash_system_asynchronous_metric_LogDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='log_size-{{instance}}',
      ),
    ],
    description='The disk usage of PageStorage instances in each TiFlash node',
    fill=0,
    decimals=1,
    legendMax=false,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/^valid_rate/', yaxis: 2 },
      { alias: '/size/', linewidth: 3 },
    ],
    yLeft='bytes',
    yRight='percentunit',
    yRightMin='0',
    yRightMax='1.1',
    yRightShow=true,
  );

  local panelPagestorageFileNum = common.graph(
    'PageStorage File Num',
    [
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_BlobFileNums', common.selector, by=['instance']),
        'blob_file-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_asynchronous_metric_LogNums', common.selector, by=['instance']),
        'log_file-{{instance}}',
      ),
    ],
    description='The number of files of PageStorage instances in each TiFlash node',
    fill=0,
    decimals=1,
    legendMax=false,
    legendSort=null,
    legendSortDesc=null,
    yLeft='short',
    yRight='percentunit',
    yRightMin='0',
    yRightMax='1.1',
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
    show=false,
  );

  local panelPageGcDuration = common.durationPanel(
    'Page GC Duration',
    'tiflash_storage_page_gc_duration_seconds_bucket',
    by=['type'],
    legend='{{type}}-%s {{$additional_groupby}}',
  );

  local panelNumerOfPages = common.graph(
    'Numer of Pages',
    [
      prometheus.target(
        'tiflash_system_asynchronous_metric_PagesInMem{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='num_pages-{{instance}}',
      ),
      prometheus.target(
        'tiflash_system_asynchronous_metric_VersionedEntries{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
        legendFormat='num_entries-{{instance}}',
      ),
    ],
    description='The number of pages of all TiFlash instance',
    fill=0,
    decimals=1,
    legendMax=false,
    legendSort='current',
    yLeft='short',
    yRight='short',
  );

  local panelPagestoragePendingWritersNum = common.graph(
    'PageStorage Pending Writers Num',
    [
      common.target(
        common.expr.sum('tiflash_system_current_metric_PSPendingWriterNum', common.selector, by=['instance']),
        'size-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='The num of pending writers in PageStorage',
    fill=0,
    nullPointMode='null',
    sideWidth=250,
    yLeft='none',
    yRight='short',
  );

  local panelPagestorageStoredBytesByType = common.graph(
    'PageStorage stored bytes by type',
    [
      common.target(
        common.expr.sum('tiflash_storage_page_data_by_types', common.selector, by=['type']),
        '{{type}}',
      ),
    ],
    decimals=1,
    nullPointMode='null',
    legendHideZero=true,
    legendSort='current',
    yLeft='bytes',
    yRight='short',
    yRightMin='0',
  );

  local panelNumberOfTables = common.graph(
    'Number of Tables',
    [
      common.target(
        common.expr.sum('tiflash_system_current_metric_StoragePoolV2Only', common.selector, by=['instance']),
        'V2-{{instance}}',
        hide=true,
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_StoragePoolV3Only', common.selector, by=['instance']),
        'V3-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_StoragePoolMixMode', common.selector, by=['instance']),
        'Mix-{{instance}}',
        hide=true,
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_StoragePoolUniPS', common.selector, by=['instance']),
        'UniPS-{{instance}}',
      ),
    ],
    description='The number of tables running under different mode in DeltaTree',
    fill=0,
    decimals=1,
    legendMax=false,
    legendHideZero=true,
    legendSort='current',
    yLeft='short',
    yLeftMin=null,
    yRight='short',
  );

  local panelPsCommandOpsByInstance = common.graph(
    'PS Command OPS By Instance',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_page_command_count', common.selector, by=['instance', 'type'], range='1m'),
        '{{type}}-{{instance}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { yaxis: 2 },
    ],
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
    yRightShow=true,
  );

  local panelPsApplyEditsOpsByInstance = common.graph(
    'PS Apply edits OPS By Instance',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_page_apply_edit_type', common.selector, by=['instance', 'type'], range='1m'),
        '{{type}}-{{instance}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { yaxis: 2 },
    ],
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
    yRightShow=true,
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

  local panelIOLimiterThroughput = common.graph(
    'I/O Limiter Throughput',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_io_limiter', common.selector, by=['type', 'instance'], range='1m'),
        '{{type}}-{{instance}}',
      ),
    ],
    description='The storage I/O limiter metrics.',
    pointradius=2,
    legendHideZero=true,
    legendSort='current',
    yLeft='binBps',
    yLeftDecimals=0,
    yRight='short',
  );

  local panelIOLimiterThreshold = common.graph(
    'I/O Limiter Threshold',
    [
      common.target(
        common.expr.sum('tiflash_storage_io_limiter_curr', common.selector, by=['type', 'instance']),
        '{{type}}-{{instance}}',
      ),
    ],
    description='Current limit bytes per second of Storage I/O limiter',
    pointradius=2,
    legendMax=false,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    yLeft='bytes',
    yLeftMin=null,
    yLeftDecimals=0,
    yRight='short',
  );

  local panelIOLimiterCurrentPendingGauge = common.graph(
    'I/O Limiter Current Pending Gauge',
    [
      prometheus.target(
        'avg(tiflash_system_current_metric_RateLimiterPendingWriteRequest{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
        legendFormat='other-current-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'avg(tiflash_system_current_metric_IOLimiterPendingBgWriteReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
        legendFormat='bgwrite-current-{{instance}}',
      ),
      prometheus.target(
        'avg(tiflash_system_current_metric_IOLimiterPendingFgWriteReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
        legendFormat='fgwrite-current-{{instance}}',
      ),
      prometheus.target(
        'avg(tiflash_system_current_metric_IOLimiterPendingBgReadReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
        legendFormat='bgread-current-{{instance}}',
      ),
      prometheus.target(
        'avg(tiflash_system_current_metric_IOLimiterPendingFgReadReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
        legendFormat='fgread-current-{{instance}}',
      ),
    ],
    description='I/O Limiter current pending gauge.',
    nullPointMode='null',
    pointradius=2,
    legendHideZero=true,
    seriesOverrides=[
      { alias: '/pending/', yaxis: 2 },
    ],
    yLeft='short',
    yLeftMin=null,
    yLeftDecimals=0,
    yRight='s',
    yRightShow=true,
  );

  local panelIOLimiterPendingOps = common.graph(
    'I/O Limiter Pending OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_io_limiter_pending_count', common.selector, by=['type', 'instance'], range='1m'),
        '{{type}}-{{instance}}',
      ),
    ],
    description='The storage I/O limiter metrics.',
    pointradius=2,
    legendHideZero=true,
    seriesOverrides=[
      { alias: '', yaxis: 2 },
    ],
    yLeft='ops',
    yLeftDecimals=0,
    yRight='s',
    yRightShow=true,
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
    seriesOverrides=[
      common.override('99-delta_merge', yaxis=2),
    ],
  );

  local panelWriteDeltaManagementThroughput = common.graph(
    'Write & Delta Management Throughput',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_throughput_bytes', common.selector, labels='type=~"write|ingest"', range='1m'),
        'write+ingest',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_storage_throughput_bytes', common.selector, labels='type!~"write|ingest"', range='1m'),
        'ManageDelta',
        intervalFactor=1,
      ),
    ],
    description='The throughput of write and delta\'s background management',
    fill=0,
    decimals=1,
    nullPointMode='null',
    sideWidth=250,
    yLeft='binBps',
    yRight='bytes',
  );

  local panelWriteDeltaManagementTotal = common.graph(
    'Write & Delta Management Total',
    [
      common.target(
        common.expr.sum('tiflash_storage_throughput_bytes', common.selector, labels='type=~"write|ingest"'),
        'write+ingest',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_storage_throughput_bytes', common.selector, labels='type!~"write|ingest"'),
        'ManageDelta',
        intervalFactor=1,
      ),
    ],
    description='The throughput of write and delta\'s background management',
    fill=0,
    decimals=1,
    nullPointMode='null',
    sideWidth=250,
    yLeft='bytes',
    yRight='bytes',
  );

  local panelWriteThroughputByInstance = common.graph(
    'Write Throughput By Instance',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_throughput_bytes', common.selector, labels='type=~"write"', by=['instance'], range='1m'),
        'write-{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_storage_throughput_bytes', common.selector, labels='type=~"ingest"', by=['instance'], range='1m'),
        'ingest-{{instance}}',
      ),
    ],
    description='The throughput of write by instance',
    fill=0,
    decimals=1,
    nullPointMode='null',
    legendHideZero=true,
    sideWidth=250,
    seriesOverrides=[
      { alias: '/total/', yaxis: 2 },
    ],
    yLeft='binBps',
    yRight='bytes',
    yRightShow=true,
  );

  local panelWriteCommandOpsByInstance = common.graph(
    'Write Command OPS By Instance',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_DMWriteBlock', common.selector, by=['instance', 'type'], range='1m'),
        'write block-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
        legendFormat='{{type}}-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='The total count of different kinds of commands received',
    fill=0,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/delete_range|ingest/', yaxis: 2 },
    ],
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
    yRightShow=true,
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

  local panelKeysFlow = common.graph(
    'Keys flow',
    [
      common.target(
        common.expr.sumRate('tiflash_raft_process_keys', common.selector, by=['type'], range='1m'),
        '{{type}}',
      ),
    ],
    description='The keys flow of different kinds of Raft operations',
    decimals=1,
    nullPointMode='null',
    legendSort='current',
    yLeft='short',
    yRight='short',
    yRightMin='0',
  );

  local panelRaftThroughput = common.graph(
    'Raft throughput',
    [
      common.target(
        common.expr.sumRate('tiflash_raft_throughput_bytes', common.selector, by=['type'], range='1m'),
        '{{type}}',
      ),
    ],
    decimals=1,
    nullPointMode='null',
    legendSort='current',
    yLeft='short',
    yRight='short',
    yRightMin='0',
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
    common.target(
      common.expr.sumRate('tiflash_proxy_tikv_server_raft_append_rejects', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tiflash_role"', by=['instance'], range='1m'),
      '{{instance}}',
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
    show=false,
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

  local panelApplyingSnapshotsCount = common.graph(
    'Applying snapshots Count',
    [
      common.target(
        common.expr.sum('tiflash_system_current_metric_RaftNumSnapshotsPendingApply', common.selector, by=['instance']),
        'Pending-{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_RaftNumPrehandlingSubTasks', common.selector, by=['instance']),
        'PrehandleSubtasks-{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_RaftNumParallelPrehandlingTasks', common.selector, by=['instance']),
        'ParallelTasks-{{instance}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_RaftNumWaitedParallelPrehandlingTasks', common.selector, by=['instance']),
        'Pending-ParallelTasks-{{instance}}',
        intervalFactor=1,
      ),
    ],
    description='The number of currently applying snapshots.',
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='short',
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
    common.target(
      common.expr.sumRate('tiflash_raft_ongoing_snapshot_total_bytes', common.selector, by=['type']),
      '{{le}}',
    )
  )
  .resetYaxes()
  .addYaxis(
    format='bytes',
  )
  .addYaxis(
    format='short',
    show=false,
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

  local panelRoughSetFilterRate = common.graph(
    'Rough Set Filter Rate',
    [
      prometheus.target(
        'avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]) - rate(tiflash_system_profile_event_DMFileFilterAftRoughSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) / (rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (instance)',
        legendFormat='1min-{{instance}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) - rate(tiflash_system_profile_event_DMFileFilterAftRoughSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m])) / (rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]))) by (instance)',
        legendFormat='5min-{{instance}}',
        intervalFactor=1,
        hide=true,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_DMFileFilterNoFilter', common.selector, by=['instance'], range='1m'),
        'No Filter-{{instance}}',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_DMFileFilterAftPKAndPackSet', common.selector, by=['instance'], range='1m'),
        'PK Filter-{{instance}}',
        hide=true,
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_DMFileFilterAftRoughSet', common.selector, by=['instance'], range='1m'),
        'RS Filter-{{instance}}',
        hide=true,
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendMax=false,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/^RS Filter/', yaxis: 2 },
      { alias: '/^PK/', yaxis: 2 },
      { alias: '/^No Filter/', yaxis: 2 },
    ],
    yLeft='percentunit',
    yRight='short',
    yRightShow=true,
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

  local panelCheckpointUploadFlow = common.graph(
    'Checkpoint Upload flow',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_checkpoint_flow', common.selector, labels='type="incremental"', by=['$additional_groupby'], range='1m'),
        'incremental {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_storage_checkpoint_flow', common.selector, labels='type="compaction"', by=['$additional_groupby'], range='1m'),
        'compaction {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    description='The flow of checkpoint operations',
    decimals=1,
    nullPointMode='null',
    legendHideZero=true,
    legendSort='current',
    yLeft='binBps',
    yRight='short',
    yRightMin='0',
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

  local panelCheckpointUploadFlowByTypeIncrementalCompaction = common.graph(
    'Checkpoint Upload flow by type (incremental+compaction)',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_checkpoint_flow_by_types', common.selector, by=['type', '$additional_groupby'], range='1m'),
        '{{type}} {{$additional_groupby}}',
      ),
    ],
    description='The flow of checkpoint operations. Group by key types',
    decimals=1,
    nullPointMode='null',
    legendHideZero=true,
    legendSort='current',
    yLeft='binBps',
    yRight='short',
    yRightMin='0',
  );

  local panelRemoteFileNum = common.graph(
    'Remote File Num',
    [
      common.target(
        common.expr.sum('tiflash_storage_remote_stats', common.selector, labels='type="num_files"', by=['instance']),
        'checkpoint_data-{{instance}}',
      ),
    ],
    description='The number of files of owned by each TiFlash node',
    fill=0,
    decimals=1,
    legendMax=false,
    legendSort=null,
    legendSortDesc=null,
    yLeft='short',
    yRight='percentunit',
    yRightMin='0',
    yRightMax='1.1',
  );

  local panelRemoteStoreUsage = common.graph(
    'Remote Store Usage',
    [
      common.target(
        common.expr.sum('tiflash_storage_remote_stats', common.selector, labels='type="total_size"', by=['instance']),
        'remote_size-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_storage_remote_stats', common.selector, labels='type="valid_size"', by=['instance']),
        'valid_size-{{instance}}',
      ),
      prometheus.target(
        'sum((tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="valid_size"}) / (tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="total_size"})) by (instance)',
        legendFormat='valid_rate-{{instance}}',
        hide=true,
      ),
    ],
    description='The remote store usage owned by each TiFlash node',
    fill=0,
    decimals=1,
    legendMax=false,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/^valid_rate/', yaxis: 2 },
      { alias: '/size/', linewidth: 3 },
    ],
    yLeft='bytes',
    yRight='percentunit',
    yRightMin='0',
    yRightMax='1.1',
    yRightShow=true,
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

  local panelRemoteStoreSummary = common.graph(
    'Remote Store Summary',
    [
      common.target(
        common.expr.sum('tiflash_storage_s3_store_summary_bytes', common.selector, by=['instance', 'store_id', 'type']),
        'store-{{store_id}}-{{type}}',
      ),
    ],
    fill=0,
    decimals=1,
    legendMax=false,
    legendHideZero=true,
    legendSort='current',
    yLeft='bytes',
    yRight='short',
  );

  local panelRemoteGcDurationBreakdown = common.durationPanel(
    'Remote GC Duration Breakdown',
    'tiflash_storage_s3_gc_seconds_bucket',
    by=['type'],
    legend='%s-{{type}} {{$additional_groupby}}',
    seriesOverrides=[
      common.override('/total/', yaxis=2),
      common.override('/one_store/', yaxis=2),
      common.override('/clean_locks/', yaxis=2),
    ],
  );

  local panelRemoteGcStatus = common.graph(
    'Remote GC Status',
    [
      common.target(
        common.expr.sum('tiflash_storage_s3_gc_status', common.selector, by=['instance', 'type']),
        '{{instance}}-{{type}}',
      ),
    ],
    fill=0,
    decimals=1,
    legendMax=false,
    legendHideZero=true,
    legendSort='current',
    yLeft='short',
    yRight='short',
  );

  local panelLocalLockManagerStatus = common.graph(
    'Local Lock Manager status',
    [
      common.target(
        common.expr.sum('tiflash_storage_s3_lock_mgr_status', common.selector, by=['instance', 'type']),
        '{{instance}}-{{type}}',
      ),
    ],
    fill=0,
    decimals=1,
    legendMax=false,
    legendHideZero=true,
    legendSort='current',
    yLeft='short',
    yRight='short',
  );

  local panelLocalLockManagerQps = common.opsPanel(
    'Local Lock Manager QPS',
    'tiflash_storage_s3_lock_mgr_counter',
    by=['type', '$additional_groupby'],
    legend='{{type}} {{$additional_groupby}}',
    yLeft='none',
  );

  local panelFapResult = common.graph(
    'FAP result',
    [
      common.target(
        common.expr.sumRate('tiflash_fap_task_result', common.selector, by=['type', '$additional_groupby'], range='1m'),
        '{{type}} {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendMax=false,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/hit_ratio/', yaxis: 2 },
    ],
    yLeft='ops',
    yRight='percentunit',
    yRightMin='0',
    yRightShow=true,
  );

  local panelFapState = common.graph(
    'FAP state',
    [
      common.target(
        common.expr.sumRate('tiflash_fap_task_state', common.selector, by=['type', '$additional_groupby'], range='1m'),
        '{{type}} {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendMax=false,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/hit_ratio/', yaxis: 2 },
    ],
    yLeft='ops',
    yRight='percentunit',
    yRightMin='0',
    yRightShow=true,
  );

  local panelFapTimeByStage = common.durationPanel(
    'FAP time by stage',
    'tiflash_fap_task_duration_seconds_bucket',
    by=['type'],
    legend='{{type}}-%s {{$additional_groupby}}',
    seriesOverrides=[
      common.override('/hit_ratio/', yaxis=2),
    ],
  );

  local panelFapNoMatchReason = common.graph(
    'FAP no match reason',
    [
      common.target(
        common.expr.sumRate('tiflash_fap_nomatch_reason', common.selector, by=['type', '$additional_groupby'], range='1m'),
        '{{type}} {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendMax=false,
    legendSort=null,
    legendSortDesc=null,
    seriesOverrides=[
      { alias: '/hit_ratio/', yaxis: 2 },
    ],
    yLeft='ops',
    yRight='percentunit',
    yRightMin='0',
    yRightShow=true,
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

  local panelRemoteCacheFlow = common.graph(
    'Remote Cache Flow',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_remote_cache_bytes', common.selector, by=['type', '$additional_groupby'], range='1m'),
        '{{type}} {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    description='Remote Cache Flow',
    fill=0,
    legendHideZero=true,
    yLeft='binBps',
    yRight='percentunit',
    yRightMin='0',
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

  local panelRemoteCacheWaitOnDownloadingOps = common.graph(
    'Remote Cache Wait on Downloading OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_remote_cache_wait_on_downloading_result', common.selector, by=['result', 'file_type', '$additional_groupby'], range='1m'),
        '{{result}}-{{file_type}} {{$additional_groupby}}',
      ),
    ],
    pointradius=2,
    legendHideZero=true,
    seriesOverrides=[
      { alias: '', yaxis: 2 },
    ],
    yLeft='ops',
    yLeftDecimals=0,
    yRight='s',
    yRightShow=true,
  );

  local panelRemoteCacheWaitOnDownloadingFlow = common.graph(
    'Remote Cache Wait on Downloading Flow',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_remote_cache_wait_on_downloading_bytes', common.selector, by=['result', 'file_type', '$additional_groupby'], range='1m'),
        '{{result}}-{{file_type}} {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendHideZero=true,
    yLeft='binBps',
    yRight='percentunit',
    yRightMin='0',
  );

  local panelRemoteCacheGauge = common.graph(
    'Remote Cache Gauge',
    [
      common.target(
        common.expr.sum('tiflash_storage_remote_cache_status', common.selector, by=['type', 'instance']),
        '{{type}}-{{instance}}',
      ),
    ],
    pointradius=2,
    legendHideZero=true,
    yLeft='short',
    yLeftMin=null,
    yLeftDecimals=0,
    yRight='short',
  );

  local panelRemoteCacheRejectDownloadTypeOps = common.graph(
    'Remote Cache Reject Download Type OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_remote_cache_reject', common.selector, by=['reason', 'file_type', '$additional_groupby'], range='1m'),
        '{{reason}}-{{file_type}} {{$additional_groupby}}',
      ),
    ],
    pointradius=2,
    legendHideZero=true,
    seriesOverrides=[
      { alias: '', yaxis: 2 },
    ],
    yLeft='ops',
    yLeftDecimals=0,
    yRight='s',
    yRightShow=true,
  );

  local panelRemoteCacheUsage = common.graph(
    'Remote Cache Usage',
    [
      common.target(
        common.expr.sum('tiflash_system_current_metric_DTFileCacheCapacity', common.selector, by=['instance']),
        'DTFileCapacity-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_DTFileCacheUsed', common.selector, by=['instance']),
        'DTFileUsed-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_PageCacheCapacity', common.selector, by=['instance']),
        'PageCapacity-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_PageCacheUsed', common.selector, by=['instance']),
        'PageUsed-{{instance}}',
      ),
    ],
    description='Remote Cache Usage',
    fill=0,
    legendMax=false,
    legendHideZero=true,
    legendSort='current',
    yLeft='bytes',
    yRight='percentunit',
    yRightMin='0',
  );

  local panelMemoryUsageOfStorageTasks = common.graph(
    'Memory Usage of Storage Tasks',
    [
      common.target(
        common.expr.sum('tiflash_system_current_metric_MemoryTrackingQueryStorageTask', common.selector, by=['instance']),
        'MemoryTrackingQueryStorageTask-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_MemoryTrackingFetchPages', common.selector, by=['instance']),
        'MemoryTrackingFetchPages-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_DT_DeltaIndexCacheSize', common.selector, by=['instance']),
        'DeltaIndexCacheSize-{{instance}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_MemoryTrackingSharedColumnData', common.selector, by=['instance']),
        'SharedColumnData-{{instance}}',
      ),
    ],
    description='Memory Usage of Storage Tasks',
    fill=0,
    legendCurrent=false,
    legendHideZero=true,
    legendSort=null,
    legendSortDesc=null,
    yLeft='bytes',
    yRight='percentunit',
    yRightMin='0',
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

  local panelPlaceindextaskReuseOps = common.graph(
    'PlaceIndexTask/Reuse OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_place_index_count', common.selector, by=['type', '$additional_groupby']),
        '{{type}} {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_storage_subtask_count', common.selector, labels='type=~"place_index_update"', by=['type', '$additional_groupby']),
        '{{type}} {{$additional_groupby}}',
      ),
    ],
    description='Total number of storage\'s internal sub tasks',
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='ops',
    yLeftDecimals=1,
    yRight='opm',
    yRightMin='0',
  );

  local panelPlaceindexUpdateRowsDeletes = common.graph(
    'PlaceIndex update rows/deletes',
    [
      prometheus.target(
        'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_place_index_stats_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, $additional_groupby) / 1000000000)',
        legendFormat='max {{$additional_groupby}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'histogram_quantile(0.99, sum(rate(tiflash_storage_place_index_stats_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type, $additional_groupby))',
        legendFormat='99-{{type}} {{$additional_groupby}}',
        intervalFactor=1,
        hide=true,
      ),
      prometheus.target(
        'sum by (type, $additional_groupby) (rate(tiflash_storage_place_index_stats_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) / sum by (type, $additional_groupby) (rate(tiflash_storage_place_index_stats_count_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
        legendFormat='avg-{{type}} {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    yLeft='short',
    yRight='opm',
    yRightMin='0',
    yRightDecimals=2,
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

  local panelS3Bytes = common.graph(
    'S3 Bytes',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3WriteBytes', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3WriteBytes {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3ReadBytes', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3ReadBytes {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3WriteDMFileBytes', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3WriteDMFileBytes {{$additional_groupby}}',
      ),
    ],
    description='S3 read/write throughput',
    fill=0,
    yLeft='binBps',
    yRight='opm',
    yRightMin='0',
  );

  local panelS3Ops = common.graph(
    'S3 OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3PutObject', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3PutObject {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3GetObject', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3GetObject {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3HeadObject', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3HeadObject {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3ListObjects', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3ListObjects {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3DeleteObject', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3DeleteObject {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3CopyObject', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3CopyObject {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3CreateMultipartUpload', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3CreateMultipartUpload {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3UploadPart', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3UploadPart {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3CompleteMultipartUpload', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3CompleteMultipartUpload {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3PutDMFile', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3PutDMFile {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IORead', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IORead {{$additional_groupby}}',
        hide=true,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IOSeek', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IOSeek {{$additional_groupby}}',
        hide=true,
      ),
    ],
    description='S3 OPS',
    fill=0,
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
  );

  local panelS3RetryOps = common.graph(
    'S3 Retry OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3GetObjectRetry', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3GetObjectRetry {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3PutObjectRetry', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3PutObjectRetry {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3PutDMFileRetry', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3PutDMFileRetry {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IOReadError', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IOReadError {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IOSeekError', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IOSeekError {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IOSeekBackward', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IOSeekBackward {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    description='S3 Retry OPS',
    fill=0,
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
  );

  local panelS3RequestDuration = common.durationPanel(
    'S3 Request Duration',
    'tiflash_storage_s3_request_seconds_bucket',
    by=['type'],
    legend='{{type}}-%s {{$additional_groupby}}',
    description='S3 Request Duration',
  );

  local panelS3HttpOps = common.graph(
    'S3 HTTP OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3ReadRequestsCount', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'read-count {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3WriteRequestsCount', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'write-count {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3ReadRequestsErrors', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'read-error {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3WriteRequestsErrors', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'write-error {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3ReadRequestsThrottling', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'read-throttling {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3WriteRequestsThrottling', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'write-throttling {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3ReadRequestsRedirects', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'read-redirects {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3WriteRequestsRedirects', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'write-redirects {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3ReadRequestsNotFound', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'read-notfound {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3WriteRequestsNotFound', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'write-notfound {{$additional_groupby}}',
      ),
    ],
    description='S3 HTTP OPS',
    fill=0,
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
  );

  local panelS3HttpRequestDuration = common.durationPanel(
    'S3 HTTP Request Duration',
    'tiflash_storage_s3_http_request_seconds_bucket',
    by=['type'],
    legend='{{type}}-%s {{$additional_groupby}}',
    description='S3 HTTP Request Duration',
  );

  local panelS3OnGoingInstances = common.graph(
    'S3 on-going instances',
    [
      common.target(
        common.expr.sum('tiflash_system_current_metric_S3Requests', common.selector, by=['type', '$additional_groupby']),
        'S3Requests {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sum('tiflash_system_current_metric_S3RandomAccessFile', common.selector, by=['type', '$additional_groupby']),
        'S3RandomAccessFile {{$additional_groupby}}',
      ),
    ],
    description='S3 HTTP OPS',
    fill=0,
    yLeft='none',
    yRight='opm',
    yRightMin='0',
  );

  local panelS3randomaccessfileOps = common.graph(
    'S3RandomAccessFile OPS',
    [
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IOReadError', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IOReadError {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IOSeekError', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IOSeekError {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IOSeekBackward', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IOSeekBackward {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IORead', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IORead {{$additional_groupby}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sumRate('tiflash_system_profile_event_S3IOSeek', common.selector, by=['type', '$additional_groupby'], range='1m'),
        'S3IOSeek {{$additional_groupby}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    yLeft='ops',
    yRight='opm',
    yRightMin='0',
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

  local panelTaskThreadPoolSize = common.graph(
    'Task Thread Pool Size',
    [
      prometheus.target(
        'max(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_task_thread_pool_size"}) by (instance, type)',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='short',
  );

  local panelTaskCount = common.graph(
    'Task Count',
    [
      prometheus.target(
        'max(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_tasks_count"}) by (instance, type)',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_pipeline_scheduler', common.selector, labels='type=~".*_tasks_count"', by=['type']),
        'sum({{type}})',
      ),
    ],
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='short',
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

  local panelTaskMaxExecuteTimePerRound = common.graph(
    'Task Max Execute Time Per Round',
    [
      prometheus.target(
        'histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
        legendFormat='95-{{type}}',
      ),
      prometheus.target(
        'histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
        legendFormat='99-{{type}}',
      ),
      prometheus.target(
        'histogram_quantile(0.99, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
        legendFormat='999-{{type}}',
      ),
      prometheus.target(
        'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type) / 1000000000)',
        legendFormat='100-{{type}}',
      ),
      prometheus.target(
        'sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu"}[1m])) / sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu"}[1m]))',
        legendFormat='avg-cpu',
      ),
      prometheus.target(
        'sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io"}[1m])) / sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io"}[1m]))',
        legendFormat='avg-io',
      ),
    ],
    legendSort='current',
    yLeft='s',
    yRight='short',
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

  local panelWaitNotifyTaskDetails = common.graph(
    'Wait notify task details',
    [
      prometheus.target(
        'max(tiflash_pipeline_wait_on_notify_tasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
      common.target(
        common.expr.sum('tiflash_pipeline_wait_on_notify_tasks', common.selector, by=['type']),
        'sum({{type}})',
      ),
    ],
    description='wait notify task details',
    fill=0,
    legendSort=null,
    legendSortDesc=null,
    yLeft='none',
    yRight='short',
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

  local panelTiflashResourceGroup = common.graph(
    'TiFlash Resource Group',
    [
      prometheus.target(
        'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="remaining_tokens", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
        legendFormat='remaining_tokens-{{instance}}-{{resource_group}}',
      ),
      prometheus.target(
        'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="avg_speed", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
        legendFormat='avg_speed-{{instance}}-{{resource_group}}',
        hide=true,
      ),
      common.target(
        common.expr.sumRate('tiflash_resource_group_counter', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="total_consumption", instance=~"$instance", instance=~"$tiflash_role"', by=['instance', 'resource_group'], range='1m'),
        'total_consumption-{{instance}}-{{resource_group}}',
        hide=true,
      ),
      prometheus.target(
        'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="bucket_fill_rate", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
        legendFormat='bucket_fill_rate-{{instance}}-{{resource_group}}',
        hide=true,
      ),
      prometheus.target(
        'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="bucket_capacity", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
        legendFormat='bucket_capacity-{{instance}}-{{resource_group}}',
        hide=true,
      ),
      common.target(
        common.expr.sumRate('tiflash_resource_group_counter', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="request_gac_count", instance=~"$instance", instance=~"$tiflash_role"', by=['instance', 'resource_group'], range='1m'),
        'request_gac_count-{{instance}}-{{resource_group}}',
        hide=true,
      ),
      common.target(
        common.expr.sumRate('tiflash_resource_group_counter', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="gac_req_ru_consumption_delta", instance=~"$instance", instance=~"$tiflash_role"', by=['instance', 'resource_group'], range='1m'),
        'gac_req_ru_consumption_delta-{{instance}}-{{resource_group}}',
        hide=true,
      ),
      common.target(
        common.expr.sumRate('tiflash_resource_group_counter', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="compute_ru_consumption", instance=~"$instance", instance=~"$tiflash_role"', by=['instance', 'resource_group'], range='1m'),
        'compute_ru_consumption-{{instance}}-{{resource_group}}',
        hide=true,
      ),
      common.target(
        common.expr.sumRate('tiflash_resource_group_counter', 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="storage_ru_consumption", instance=~"$instance", instance=~"$tiflash_role"', by=['instance', 'resource_group'], range='1m'),
        'storage_ru_consumption-{{instance}}-{{resource_group}}',
        hide=true,
      ),
    ],
    description='Metas of resource group',
    nullPointMode='null',
    pointradius=2,
    legendMax=false,
    legendAvg=true,
    legendSort=null,
    legendSortDesc=null,
    yLeft='short',
    yLeftMin=null,
    yRight='short',
  );

  local panelRequestUnit = common.graph(
    'Request Unit',
    [
      common.target(
        common.expr.sumRate('tiflash_storage_sync_replica_ru', 'instance=~"$tiflash_role"', by=['keyspace_id', '$additional_groupby'], range='1m'),
        'replica-sync-rate-{{keyspace_id}}',
      ),
      prometheus.target(
        'sum(increase(tiflash_storage_sync_replica_ru{instance=~"$tiflash_role"}[24h])) by (keyspace_id, $additional_groupby)',
        legendFormat='replica-sync-sum-24h-{{keyspace_id}} {{$additional_groupby}}',
      ),
      common.target(
        common.expr.sumRate('tiflash_compute_request_unit', 'instance=~"$tiflash_role"', by=['cluster_id', '$additional_groupby'], range='1m'),
        'query-rate-{{cluster_id}} {{$additional_groupby}}',
      ),
      prometheus.target(
        'sum(increase(tiflash_compute_request_unit{instance=~"$tiflash_role"}[24h])) by (cluster_id, $additional_groupby)',
        legendFormat='query-sum-24h-{{cluster_id}} {{$additional_groupby}}',
      ),
      prometheus.target(
        'sum(rate(tiflash_storage_ru_read_bytes{instance=~"$tiflash_role"}[1m])) by (keyspace, resource_group, type, $additional_groupby) / (64 * 1024)',
        legendFormat='storage-{{keyspace}}_{{resource_group}}_{{type}} {{$additional_groupby}}',
      ),
    ],
    description='Request Unit for tidb-serverless charging',
    fill=0,
    decimals=1,
    legendMax=false,
    legendSort='current',
    seriesOverrides=[
      { alias: '/sum/', yaxis: 2 },
    ],
    yLeft='cps',
    yRight='short',
    yRightMin='0',
    yRightShow=true,
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

  local panelStatusApiRequestOpS = common.graph(
    'Status API Request (op/s)',
    [
      prometheus.target(
        'sum(rate( tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_count {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"} [$__rate_interval] )) by (path, $additional_groupby)',
        legendFormat='{{path}} {{$additional_groupby}}',
      ),
    ],
    nullPointMode='null',
    legendHideZero=true,
    legendHideEmpty=true,
    legendSort=null,
    legendSortDesc=null,
    yLeft='ops',
    yRight='short',
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

  local panelInMemoryVectorIndexInstances = common.graph(
    'In-Memory Vector Index Instances',
    [
      prometheus.target(
        'sum by (type, instance) ( tiflash_vector_index_active_instances{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role" } )',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    decimals=0,
    legendCurrent=false,
    legendHideZero=true,
    legendHideEmpty=true,
    yLeft='short',
    yLeftDecimals=0,
    yRight='ops',
    yRightMin='0',
  );

  local panelVectorIndexEstimatedMemoryUsage = common.graph(
    'Vector Index Estimated Memory Usage',
    [
      prometheus.target(
        'tiflash_vector_index_memory_usage{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role" }',
        legendFormat='{{instance}}-{{type}}',
        intervalFactor=1,
      ),
      prometheus.target(
        'tiflash_process_rss_by_type_bytes{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="file" }',
        legendFormat='{{instance}}-RssFile',
      ),
    ],
    fill=0,
    decimals=0,
    legendCurrent=false,
    legendHideZero=true,
    legendHideEmpty=true,
    yLeft='bytes',
    yLeftDecimals=0,
    yRight='ops',
    yRightMin='0',
  );

  local panelP999VectorSearchDurationPerRequest = common.graph(
    '99.9% Vector Search Duration (Per Request)',
    [
      prometheus.target(
        'histogram_quantile( 0.999, sum(rate( tiflash_vector_index_duration_bucket{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!="build" } [$__rate_interval] )) by (le, type) )',
        legendFormat='{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    decimals=1,
    legendCurrent=false,
    seriesOverrides=[
      { alias: '/download/', yaxis: 2 },
    ],
    yLeft='s',
    yLeftDecimals=1,
    yRight='s',
    yRightMin='0',
    yRightDecimals=1,
    yRightShow=true,
  );

  local panelP999VectorIndexBuildDurationPerDmfileColumn = common.graph(
    '99.9% Vector Index Build Duration (Per DMFile Column)',
    [
      prometheus.target(
        'histogram_quantile( 0.999, sum(rate( tiflash_vector_index_duration_bucket{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="build" } [$__rate_interval] )) by (le, type) )',
        legendFormat='{{type}}',
        intervalFactor=1,
      ),
    ],
    fill=0,
    decimals=1,
    legendCurrent=false,
    yLeft='s',
    yLeftDecimals=1,
    yRight='s',
    yRightMin='0',
    yRightDecimals=1,
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
