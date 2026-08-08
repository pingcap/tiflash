// Generated from tiflash_summary.json — edit carefully or regenerate.
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
  formatY1='none',
  formatY2='short',
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
);

local threads_IOP = graphPanel.new(
  title='Threads IO',
  datasource=common.datasource,
  formatY1='Bps',
  formatY2='short',
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
);

local thread_Voluntary_Context_SwitchesP = graphPanel.new(
  title='Thread Voluntary Context Switches',
  datasource=common.datasource,
  formatY1='none',
  formatY2='short',
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
);

local thread_Nonvoluntary_Context_SwitchesP = graphPanel.new(
  title='Thread Nonvoluntary Context Switches',
  datasource=common.datasource,
  formatY1='none',
  formatY2='short',
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
);


{
  row: rowObj
  .addPanel(threads_stateP, gridPos=common.pos(12, 7))
  .addPanel(threads_IOP, gridPos=common.pos(12, 7))
  .addPanel(thread_Voluntary_Context_SwitchesP, gridPos=common.pos(12, 7))
  .addPanel(thread_Nonvoluntary_Context_SwitchesP, gridPos=common.pos(12, 7))
  ,
  panels: [
    { panel: threads_stateP, w: 12, h: 7 },
    { panel: threads_IOP, w: 12, h: 7 },
    { panel: thread_Voluntary_Context_SwitchesP, w: 12, h: 7 },
    { panel: thread_Nonvoluntary_Context_SwitchesP, w: 12, h: 7 }
  ],
}
