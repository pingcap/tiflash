// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Threads');

local threads_stateP = graphPanel.new(
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

local threads_IOP = graphPanel.new(
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

local thread_Voluntary_Context_SwitchesP = graphPanel.new(
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

local thread_Nonvoluntary_Context_SwitchesP = graphPanel.new(
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


{
  row: common.buildRow(
    rowObj,
    [
      common.band([threads_stateP, threads_IOP]),
      common.band([thread_Voluntary_Context_SwitchesP, thread_Nonvoluntary_Context_SwitchesP])
    ],
  ),
}
