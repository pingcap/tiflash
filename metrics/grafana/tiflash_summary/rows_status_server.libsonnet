// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Status Server');

local status_API_Request_DurationP = graphPanel.new(
  title='Status API Request Duration',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate( tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket {k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval] )) by (le, path, $additional_groupby))',
    legendFormat='999-{{path}} {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate( tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket {k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval] )) by (le, path, $additional_groupby))',
    legendFormat='99-{{path}} {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    '(sum(rate( tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"} [$__rate_interval] )) by (path, $additional_groupby) / sum(rate( tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"} [$__rate_interval] )) by (path, $additional_groupby) )',
    legendFormat='avg-{{path}} {{$additional_groupby}}',
    intervalFactor=1,
    hide=true,
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

local status_API_Request_op_sP = graphPanel.new(
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


{
  row: rowObj
  .addPanel(status_API_Request_DurationP, gridPos=common.pos(12, 7, x=0, y=204))
  .addPanel(status_API_Request_op_sP, gridPos=common.pos(12, 7, x=12, y=204))
  ,
  panels: [
    { panel: status_API_Request_DurationP, w: 12, h: 7, x: 0, y: 204 },
    { panel: status_API_Request_op_sP, w: 12, h: 7, x: 12, y: 204 }
  ],
}
