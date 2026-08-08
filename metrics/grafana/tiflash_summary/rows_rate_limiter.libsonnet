// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Rate Limiter');

local i_O_Limiter_ThroughputP = graphPanel.new(
  title='I/O Limiter Throughput',
  datasource=common.datasource,
  description='The storage I/O limiter metrics.',
  fill=1,
  nullPointMode='null as zero',
  pointradius=2,
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
    'sum(rate(tiflash_storage_io_limiter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, instance)',
    legendFormat='{{type}}-{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
  decimals=0,
)
.addYaxis(
  format='short',
);

local i_O_Limiter_ThresholdP = graphPanel.new(
  title='I/O Limiter Threshold',
  datasource=common.datasource,
  description='Current limit bytes per second of Storage I/O limiter',
  fill=1,
  nullPointMode='null as zero',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_hideZero=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_storage_io_limiter_curr{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type, instance)',
    legendFormat='{{type}}-{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
  decimals=0,
)
.addYaxis(
  format='short',
);

local i_O_Limiter_Current_Pending_GaugeP = graphPanel.new(
  title='I/O Limiter Current Pending Gauge',
  datasource=common.datasource,
  description='I/O Limiter current pending gauge.',
  fill=1,
  nullPointMode='null',
  pointradius=2,
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
    'avg(tiflash_system_current_metric_RateLimiterPendingWriteRequest{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='other-current-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'avg(tiflash_system_current_metric_IOLimiterPendingBgWriteReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='bgwrite-current-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'avg(tiflash_system_current_metric_IOLimiterPendingFgWriteReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='fgwrite-current-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'avg(tiflash_system_current_metric_IOLimiterPendingBgReadReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='bgread-current-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'avg(tiflash_system_current_metric_IOLimiterPendingFgReadReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='fgread-current-{{instance}}',
  )
)
.addSeriesOverride({ alias: '/pending/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='short',
  decimals=0,
)
.addYaxis(
  format='s',
);

local i_O_Limiter_Pending_OPSP = graphPanel.new(
  title='I/O Limiter Pending OPS',
  datasource=common.datasource,
  description='The storage I/O limiter metrics.',
  fill=1,
  nullPointMode='null as zero',
  pointradius=2,
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
    'sum(rate(tiflash_storage_io_limiter_pending_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, instance)',
    legendFormat='{{type}}-{{instance}}',
  )
)
.addSeriesOverride({ alias: '', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
  decimals=0,
)
.addYaxis(
  format='s',
);

local i_O_Limiter_Pending_DurationP = graphPanel.new(
  title='I/O Limiter Pending Duration',
  datasource=common.datasource,
  description='I/O Limiter pending duration.',
  fill=1,
  nullPointMode='null',
  pointradius=2,
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_io_limiter_pending_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type) / 1000000000)',
    legendFormat='{{type}}-pending-max',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='{{type}}-pending-P999',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='{{type}}-pending-P99',
  )
)
.resetYaxes()
.addYaxis(
  format='s',
  decimals=0,
)
.addYaxis(
  format='s',
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([i_O_Limiter_ThroughputP, i_O_Limiter_ThresholdP]),
      common.band([i_O_Limiter_Current_Pending_GaugeP, i_O_Limiter_Pending_OPSP, i_O_Limiter_Pending_DurationP])
    ],
  ),
}
