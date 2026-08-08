// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Vector Search');

local in_Memory_Vector_Index_InstancesP = graphPanel.new(
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

local vector_Index_Estimated_Memory_UsageP = graphPanel.new(
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

local p_99_9_Vector_Search_Duration_Per_RequestP = graphPanel.new(
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

local p_99_9_Vector_Index_Build_Duration_Per_DMFile_ColumnP = graphPanel.new(
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


{
  row: common.buildRow(
    rowObj,
    [
      common.band([in_Memory_Vector_Index_InstancesP, vector_Index_Estimated_Memory_UsageP]),
      common.band([p_99_9_Vector_Search_Duration_Per_RequestP, p_99_9_Vector_Index_Build_Duration_Per_DMFile_ColumnP])
    ],
  ),
}
