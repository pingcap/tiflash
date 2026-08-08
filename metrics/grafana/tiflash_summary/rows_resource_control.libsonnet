// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='TiFlash Resource Control');

local tiFlash_Resource_GroupP = graphPanel.new(
  title='TiFlash Resource Group',
  datasource=common.datasource,
  description='Metas of resource group',
  fill=1,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_avg=true,
)
.addTarget(
  prometheus.target(
    'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="remaining_tokens", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
    legendFormat='remaining_tokens-{{instance}}-{{resource_group}}',
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="avg_speed", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
    legendFormat='avg_speed-{{instance}}-{{resource_group}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="total_consumption", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
    legendFormat='total_consumption-{{instance}}-{{resource_group}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="bucket_fill_rate", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
    legendFormat='bucket_fill_rate-{{instance}}-{{resource_group}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="bucket_capacity", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
    legendFormat='bucket_capacity-{{instance}}-{{resource_group}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="request_gac_count", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
    legendFormat='request_gac_count-{{instance}}-{{resource_group}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="gac_req_ru_consumption_delta", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
    legendFormat='gac_req_ru_consumption_delta-{{instance}}-{{resource_group}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="compute_ru_consumption", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
    legendFormat='compute_ru_consumption-{{instance}}-{{resource_group}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="storage_ru_consumption", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,resource_group)',
    legendFormat='storage_ru_consumption-{{instance}}-{{resource_group}}',
    hide=true,
  )
)
.resetYaxes()
.addYaxis(
  format='short',
)
.addYaxis(
  format='short',
);

local request_UnitP = graphPanel.new(
  title='Request Unit',
  datasource=common.datasource,
  description='Request Unit for tidb-serverless charging',
  fill=0,
  nullPointMode='null as zero',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_sync_replica_ru{instance=~"$tiflash_role"}[1m])) by (keyspace_id, $additional_groupby)',
    legendFormat='replica-sync-rate-{{keyspace_id}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(increase(tiflash_storage_sync_replica_ru{instance=~"$tiflash_role"}[24h])) by (keyspace_id, $additional_groupby)',
    legendFormat='replica-sync-sum-24h-{{keyspace_id}} {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_compute_request_unit{instance=~"$tiflash_role"}[1m])) by (cluster_id, $additional_groupby)',
    legendFormat='query-rate-{{cluster_id}} {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(increase(tiflash_compute_request_unit{instance=~"$tiflash_role"}[24h])) by (cluster_id, $additional_groupby)',
    legendFormat='query-sum-24h-{{cluster_id}} {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_ru_read_bytes{instance=~"$tiflash_role"}[1m])) by (keyspace, resource_group, type, $additional_groupby) / (64 * 1024)',
    legendFormat='storage-{{keyspace}}_{{resource_group}}_{{type}} {{$additional_groupby}}',
  )
)
.addSeriesOverride({ alias: '/sum/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='cps',
  min='0',
)
.addYaxis(
  format='short',
  min='0',
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([tiFlash_Resource_GroupP, request_UnitP])
    ],
  ),
}
