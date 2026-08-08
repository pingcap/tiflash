// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Columnar Storage');

local iA_usageP = graphPanel.new(
  title='IA usage',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
  min='0',
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
.addSeriesOverride({ alias: '/limit/', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2 });

local iA_Segments_Memory_WaitP = graphPanel.new(
  title='IA Segments Memory Wait',
  datasource=common.datasource,
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m]))) by (le, $additional_groupby) / 1000000000)',
    legendFormat='max {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='9999 {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='99 {{$additional_groupby}}',
  )
);

local iA_Segment_Remote_Read_CacheP = graphPanel.new(
  title='IA Segment Remote Read Cache',
  datasource=common.datasource,
  formatY1='ops',
  formatY2='opm',
  min='0',
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
);

local iA_Segments_Remote_Read_DurationP = graphPanel.new(
  title='IA Segments Remote Read Duration',
  datasource=common.datasource,
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m]))) by (le, $additional_groupby) / 1000000000)',
    legendFormat='max {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='9999 {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='99 {{$additional_groupby}}',
  )
);

local columnarFile_CacheP = graphPanel.new(
  title='ColumnarFile Cache',
  datasource=common.datasource,
  formatY1='ops',
  formatY2='opm',
  min='0',
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
);

local columnar_Prefetch_DurationP = graphPanel.new(
  title='Columnar Prefetch Duration',
  datasource=common.datasource,
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m]))) by (le, $additional_groupby) / 1000000000)',
    legendFormat='max {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='9999 {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='99 {{$additional_groupby}}',
  )
);

local columnar_Prefetch_Cache_Hit_DurationP = graphPanel.new(
  title='Columnar Prefetch Cache Hit Duration',
  datasource=common.datasource,
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m]))) by (le, $additional_groupby) / 1000000000)',
    legendFormat='max {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='9999 {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='99 {{$additional_groupby}}',
  )
);

local columnar_Fetch_Snapshot_RetryP = graphPanel.new(
  title='Columnar Fetch Snapshot Retry',
  datasource=common.datasource,
  formatY1='ops',
  formatY2='opm',
  min='0',
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
    'sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_retry_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by ($additional_groupby)',
    legendFormat='retry {{$additional_groupby}}',
    intervalFactor=1,
  )
);

local columnar_Fetch_Snapshot_DurationP = graphPanel.new(
  title='Columnar Fetch Snapshot Duration',
  datasource=common.datasource,
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m]))) by (le, $additional_groupby) / 1000000000)',
    legendFormat='max {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='9999 {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m])) by (le, $additional_groupby))',
    legendFormat='99 {{$additional_groupby}}',
  )
);

local columnar_Meta_CacheP = graphPanel.new(
  title='Columnar Meta Cache',
  datasource=common.datasource,
  formatY1='ops',
  formatY2='opm',
  min='0',
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
);

local columnar_Meta_Cache_GaugeP = graphPanel.new(
  title='Columnar Meta Cache Gauge',
  datasource=common.datasource,
  formatY1='bytes',
  formatY2='short',
  min='0',
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
.addSeriesOverride({ alias: '/entries/', yaxis: 2 });


{
  row: rowObj
  .addPanel(iA_usageP, gridPos=common.pos(12, 8, x=0, y=9))
  .addPanel(iA_Segments_Memory_WaitP, gridPos=common.pos(12, 8, x=12, y=9))
  .addPanel(iA_Segment_Remote_Read_CacheP, gridPos=common.pos(12, 8, x=0, y=17))
  .addPanel(iA_Segments_Remote_Read_DurationP, gridPos=common.pos(12, 8, x=12, y=17))
  .addPanel(columnarFile_CacheP, gridPos=common.pos(8, 8, x=0, y=25))
  .addPanel(columnar_Prefetch_DurationP, gridPos=common.pos(8, 8, x=8, y=25))
  .addPanel(columnar_Prefetch_Cache_Hit_DurationP, gridPos=common.pos(8, 8, x=16, y=25))
  .addPanel(columnar_Fetch_Snapshot_RetryP, gridPos=common.pos(12, 8, x=0, y=33))
  .addPanel(columnar_Fetch_Snapshot_DurationP, gridPos=common.pos(12, 8, x=12, y=33))
  .addPanel(columnar_Meta_CacheP, gridPos=common.pos(12, 8, x=0, y=41))
  .addPanel(columnar_Meta_Cache_GaugeP, gridPos=common.pos(12, 8, x=12, y=41))
  ,
  panels: [
    { panel: iA_usageP, w: 12, h: 8, x: 0, y: 9 },
    { panel: iA_Segments_Memory_WaitP, w: 12, h: 8, x: 12, y: 9 },
    { panel: iA_Segment_Remote_Read_CacheP, w: 12, h: 8, x: 0, y: 17 },
    { panel: iA_Segments_Remote_Read_DurationP, w: 12, h: 8, x: 12, y: 17 },
    { panel: columnarFile_CacheP, w: 8, h: 8, x: 0, y: 25 },
    { panel: columnar_Prefetch_DurationP, w: 8, h: 8, x: 8, y: 25 },
    { panel: columnar_Prefetch_Cache_Hit_DurationP, w: 8, h: 8, x: 16, y: 25 },
    { panel: columnar_Fetch_Snapshot_RetryP, w: 12, h: 8, x: 0, y: 33 },
    { panel: columnar_Fetch_Snapshot_DurationP, w: 12, h: 8, x: 12, y: 33 },
    { panel: columnar_Meta_CacheP, w: 12, h: 8, x: 0, y: 41 },
    { panel: columnar_Meta_Cache_GaugeP, w: 12, h: 8, x: 12, y: 41 }
  ],
}
