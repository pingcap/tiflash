// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

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


{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelIaUsage, panelIaSegmentsMemoryWait]),
      common.band([panelIaSegmentRemoteReadCache, panelIaSegmentsRemoteReadDuration]),
      common.band([panelColumnarfileCache, panelColumnarPrefetchDuration, panelColumnarPrefetchCacheHitDuration]),
      common.band([panelColumnarFetchSnapshotRetry, panelColumnarFetchSnapshotDuration]),
      common.band([panelColumnarMetaCache, panelColumnarMetaCacheGauge])
    ],
  ),
}
