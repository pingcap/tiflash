// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Server');

local store_sizeP = graphPanel.new(
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

local available_sizeP = graphPanel.new(
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

local capacity_sizeP = graphPanel.new(
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

local uptimeP = graphPanel.new(
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

local regionP = graphPanel.new(
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

local cPU_UsageP = graphPanel.new(
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

local memoryP = graphPanel.new(
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

local iO_ThroughputP = graphPanel.new(
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

local remote_Store_Summary_Disagg_archP = graphPanel.new(
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


{
  row: rowObj
  .addPanel(store_sizeP, gridPos=common.pos(8, 8, x=0, y=1))
  .addPanel(available_sizeP, gridPos=common.pos(8, 8, x=8, y=1))
  .addPanel(capacity_sizeP, gridPos=common.pos(8, 8, x=16, y=1))
  .addPanel(uptimeP, gridPos=common.pos(12, 8, x=0, y=9))
  .addPanel(regionP, gridPos=common.pos(12, 8, x=12, y=9))
  .addPanel(cPU_UsageP, gridPos=common.pos(12, 8, x=0, y=17))
  .addPanel(memoryP, gridPos=common.pos(12, 8, x=12, y=17))
  .addPanel(iO_ThroughputP, gridPos=common.pos(12, 8, x=0, y=25))
  .addPanel(remote_Store_Summary_Disagg_archP, gridPos=common.pos(12, 8, x=12, y=25))
  ,
  panels: [
    { panel: store_sizeP, w: 8, h: 8, x: 0, y: 1 },
    { panel: available_sizeP, w: 8, h: 8, x: 8, y: 1 },
    { panel: capacity_sizeP, w: 8, h: 8, x: 16, y: 1 },
    { panel: uptimeP, w: 12, h: 8, x: 0, y: 9 },
    { panel: regionP, w: 12, h: 8, x: 12, y: 9 },
    { panel: cPU_UsageP, w: 12, h: 8, x: 0, y: 17 },
    { panel: memoryP, w: 12, h: 8, x: 12, y: 17 },
    { panel: iO_ThroughputP, w: 12, h: 8, x: 0, y: 25 },
    { panel: remote_Store_Summary_Disagg_archP, w: 12, h: 8, x: 12, y: 25 }
  ],
}
