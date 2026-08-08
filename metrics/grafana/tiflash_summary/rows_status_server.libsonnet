// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Status Server');

local status_API_Request_DurationP = common.durationPanel(
  'Status API Request Duration',
  'tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket',
  selector=common.proxySelector,
  by=['path'],
  legend='%s-{{path}} {{$additional_groupby}}',
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
  row: common.buildRow(
    rowObj,
    [
      common.band([status_API_Request_DurationP, status_API_Request_op_sP])
    ],
  ),
}
