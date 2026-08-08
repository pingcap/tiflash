// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

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

{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelRoughSetFilterRate, panelRoughSetFilterRateHistogram])
    ],
  ),
}
