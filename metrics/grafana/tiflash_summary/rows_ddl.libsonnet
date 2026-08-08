// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='DDL');

local schema_Internal_DDL_OPMP = graphPanel.new(
  title='Schema Internal DDL OPM',
  datasource=common.datasource,
  description='Executed DDL jobs per minute',
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'avg(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='total',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type,instance)',
    legendFormat='{{type}}-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='total-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.resetYaxes()
.addYaxis(
  format='opm',
  min='0',
)
.addYaxis(
  format='none',
);

local schema_Apply_OPMP = graphPanel.new(
  title='Schema Apply OPM',
  datasource=common.datasource,
  description='Executed DDL apply jobs per minute',
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'avg(increase(tiflash_schema_trigger_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='triggle-by-{{type}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='opm',
  min='0',
)
.addYaxis(
  format='none',
);

local schema_Apply_DurationP = graphPanel.new(
  title='Schema Apply Duration',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_schema_apply_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_schema_apply_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_schema_apply_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tiflash_schema_apply_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", keyspace!=""}[1m])) by (le, type, keyspace))',
    legendFormat='80-{{type}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_sync_schema_applying{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"$type"}) by (instance)',
    legendFormat='applying-{{instance}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/^applying/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='s',
  min='0',
)
.addYaxis(
  format='short',
  min='0',
  max='2',
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([schema_Internal_DDL_OPMP, schema_Apply_OPMP]),
      common.band([{ panel: schema_Apply_DurationP, w: 12 }])
    ],
  ),
}
