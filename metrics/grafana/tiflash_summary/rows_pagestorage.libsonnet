// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='PageStorage');

local pageStorage_Disk_UsageP = graphPanel.new(
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

local pageStorage_File_NumP = graphPanel.new(
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

local pageStorage_WriteBatch_SizeP = heatmapPanel.new(
  title='PageStorage WriteBatch Size',
  datasource=common.datasource,
  dataFormat='tsbuckets',
  yAxis_format='bytes',
  hideZeroBuckets=true,
  color_mode='spectrum',
  color_colorScheme='interpolateSpectral',
  legend_show=true,
)
.addTarget(
  prometheus.target(
    'sum(delta(tiflash_storage_page_write_batch_size_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="v3"}[1m])) by (le)',
    format='heatmap',
    legendFormat='{{le}}',
  )
);

local page_write_DurationP = graphPanel.new(
  title='Page write Duration',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type) / 1000000000)',
    legendFormat='{{type}}-max',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.999, sum(rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='{{type}}-999',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='{{type}}-99',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='{{type}}-95',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.80, sum(rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='{{type}}-80',
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

local page_GC_Tasks_OPMP = graphPanel.new(
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

local page_GC_DurationP = graphPanel.new(
  title='Page GC Duration',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
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
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_page_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type) / 1000000000)',
    legendFormat='{{type}}-max',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_page_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='{{type}}-99',
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

local numer_of_PagesP = graphPanel.new(
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

local pageStorage_Pending_Writers_NumP = graphPanel.new(
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

local pageStorage_stored_bytes_by_typeP = graphPanel.new(
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

local number_of_TablesP = graphPanel.new(
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

local pS_Command_OPS_By_InstanceP = graphPanel.new(
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

local pS_Apply_edits_OPS_By_InstanceP = graphPanel.new(
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
  row: rowObj
  .addPanel(pageStorage_Disk_UsageP, gridPos=common.pos(12, 8, x=0, y=43))
  .addPanel(pageStorage_File_NumP, gridPos=common.pos(12, 8, x=12, y=43))
  .addPanel(pageStorage_WriteBatch_SizeP, gridPos=common.pos(12, 8, x=0, y=51))
  .addPanel(page_write_DurationP, gridPos=common.pos(12, 8, x=12, y=51))
  .addPanel(page_GC_Tasks_OPMP, gridPos=common.pos(12, 8, x=0, y=59))
  .addPanel(page_GC_DurationP, gridPos=common.pos(12, 8, x=12, y=59))
  .addPanel(numer_of_PagesP, gridPos=common.pos(12, 8, x=0, y=67))
  .addPanel(pageStorage_Pending_Writers_NumP, gridPos=common.pos(12, 8, x=12, y=67))
  .addPanel(pageStorage_stored_bytes_by_typeP, gridPos=common.pos(12, 8, x=0, y=75))
  .addPanel(number_of_TablesP, gridPos=common.pos(12, 8, x=12, y=75))
  .addPanel(pS_Command_OPS_By_InstanceP, gridPos=common.pos(24, 9, x=0, y=83))
  .addPanel(pS_Apply_edits_OPS_By_InstanceP, gridPos=common.pos(24, 9, x=0, y=92))
  ,
  panels: [
    { panel: pageStorage_Disk_UsageP, w: 12, h: 8, x: 0, y: 43 },
    { panel: pageStorage_File_NumP, w: 12, h: 8, x: 12, y: 43 },
    { panel: pageStorage_WriteBatch_SizeP, w: 12, h: 8, x: 0, y: 51 },
    { panel: page_write_DurationP, w: 12, h: 8, x: 12, y: 51 },
    { panel: page_GC_Tasks_OPMP, w: 12, h: 8, x: 0, y: 59 },
    { panel: page_GC_DurationP, w: 12, h: 8, x: 12, y: 59 },
    { panel: numer_of_PagesP, w: 12, h: 8, x: 0, y: 67 },
    { panel: pageStorage_Pending_Writers_NumP, w: 12, h: 8, x: 12, y: 67 },
    { panel: pageStorage_stored_bytes_by_typeP, w: 12, h: 8, x: 0, y: 75 },
    { panel: number_of_TablesP, w: 12, h: 8, x: 12, y: 75 },
    { panel: pS_Command_OPS_By_InstanceP, w: 24, h: 9, x: 0, y: 83 },
    { panel: pS_Apply_edits_OPS_By_InstanceP, w: 24, h: 9, x: 0, y: 92 }
  ],
}
