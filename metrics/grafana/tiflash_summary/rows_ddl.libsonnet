// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='DDL');

local panelSchemaInternalDdlOpm = graphPanel.new(
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

local panelSchemaApplyOpm = graphPanel.new(
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

local panelSchemaApplyDuration = common.durationPanel(
  'Schema Apply Duration',
  'tiflash_schema_apply_duration_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
  extraTargets=[
    common.target(
      common.expr.sum(
        'tiflash_sync_schema_applying',
        common.selector + ', type=~"$type"',
        by=['instance'],
      ),
      'applying-{{instance}}',
    ),
  ],
  seriesOverrides=[
    common.override('/^applying/', yaxis=2),
  ],
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelSchemaInternalDdlOpm, panelSchemaApplyOpm]),
      common.band([{ panel: panelSchemaApplyDuration, w: 12 }])
    ],
  ),
}
