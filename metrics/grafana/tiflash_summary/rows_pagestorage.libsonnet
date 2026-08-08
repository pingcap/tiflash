// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='PageStorage');

local panelPagestorageDiskUsage = graphPanel.new(
  title='PageStorage Disk Usage',
  datasource=common.datasource,
  description='The disk usage of PageStorage instances in each TiFlash node',
  fill=0,
  nullPointMode='null as zero',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'tiflash_system_asynchronous_metric_BlobDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='blob_disk_size-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_asynchronous_metric_BlobValidBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='blob_valid_size-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum((tiflash_system_asynchronous_metric_BlobValidBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) / (tiflash_system_asynchronous_metric_BlobDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})) by (instance)',
    legendFormat='blob_valid_rate-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_asynchronous_metric_LogDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='log_size-{{instance}}',
  )
)
.addSeriesOverride({ alias: '/^valid_rate/', yaxis: 2 })
.addSeriesOverride({ alias: '/size/', linewidth: 3 })
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
  max='1.1',
);

local panelPagestorageFileNum = graphPanel.new(
  title='PageStorage File Num',
  datasource=common.datasource,
  description='The number of files of PageStorage instances in each TiFlash node',
  fill=0,
  nullPointMode='null as zero',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_asynchronous_metric_BlobFileNums{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='blob_file-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_asynchronous_metric_LogNums{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='log_file-{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
)
.addYaxis(
  format='percentunit',
  min='0',
  max='1.1',
);

local panelPagestorageWritebatchSize = common.heatmap(
  'PageStorage WriteBatch Size',
  'tiflash_storage_page_write_batch_size_bucket',
  yFormat='bytes',
  labels='type="v3"',
);

local panelPageWriteDuration = common.durationPanel(
  'Page write Duration',
  'tiflash_storage_page_write_duration_seconds_bucket',
  by=['type'],
  legend='{{type}}-%s {{$additional_groupby}}',
);

local panelPageGcTasksOpm = graphPanel.new(
  title='Page GC Tasks OPM',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(increase(tiflash_storage_page_gc_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='opm',
  min='0',
)
.addYaxis(
  format='short',
);

local panelPageGcDuration = common.durationPanel(
  'Page GC Duration',
  'tiflash_storage_page_gc_duration_seconds_bucket',
  by=['type'],
  legend='{{type}}-%s {{$additional_groupby}}',
);

local panelNumerOfPages = graphPanel.new(
  title='Numer of Pages',
  datasource=common.datasource,
  description='The number of pages of all TiFlash instance',
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
    'tiflash_system_asynchronous_metric_PagesInMem{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='num_pages-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'tiflash_system_asynchronous_metric_VersionedEntries{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
    legendFormat='num_entries-{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
)
.addYaxis(
  format='short',
);

local panelPagestoragePendingWritersNum = graphPanel.new(
  title='PageStorage Pending Writers Num',
  datasource=common.datasource,
  description='The num of pending writers in PageStorage',
  fill=0,
  nullPointMode='null',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_sort='max',
  legend_sortDesc=true,
  legend_sideWidth=250,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_PSPendingWriterNum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='size-{{instance}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  min='0',
)
.addYaxis(
  format='short',
  show=false,
);

local panelPagestorageStoredBytesByType = graphPanel.new(
  title='PageStorage stored bytes by type',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null',
  decimals=1,
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
    'sum(tiflash_storage_page_data_by_types{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type)',
    legendFormat='{{type}}',
  )
)
.resetYaxes()
.addYaxis(
  format='bytes',
  min='0',
)
.addYaxis(
  format='short',
  min='0',
);

local panelNumberOfTables = graphPanel.new(
  title='Number of Tables',
  datasource=common.datasource,
  description='The number of tables running under different mode in DeltaTree',
  fill=0,
  nullPointMode='null as zero',
  decimals=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_hideZero=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_StoragePoolV2Only{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='V2-{{instance}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_StoragePoolV3Only{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='V3-{{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_StoragePoolMixMode{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='Mix-{{instance}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_StoragePoolUniPS{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='UniPS-{{instance}}',
  )
)
.resetYaxes()
.addYaxis(
  format='short',
)
.addYaxis(
  format='short',
);

local panelPsCommandOpsByInstance = graphPanel.new(
  title='PS Command OPS By Instance',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_page_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
    legendFormat='{{type}}-{{instance}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
);

local panelPsApplyEditsOpsByInstance = graphPanel.new(
  title='PS Apply edits OPS By Instance',
  datasource=common.datasource,
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideZero=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_page_apply_edit_type{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance, type)',
    legendFormat='{{type}}-{{instance}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelPagestorageDiskUsage, panelPagestorageFileNum]),
      common.band([panelPagestorageWritebatchSize, panelPageWriteDuration]),
      common.band([panelPageGcTasksOpm, panelPageGcDuration]),
      common.band([panelNumerOfPages, panelPagestoragePendingWritersNum]),
      common.band([panelPagestorageStoredBytesByType, panelNumberOfTables]),
      common.band([panelPsCommandOpsByInstance]),
      common.band([panelPsApplyEditsOpsByInstance])
    ],
  ),
}
