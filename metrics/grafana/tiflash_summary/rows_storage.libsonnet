// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Storage');

local write_Command_OPSP = graphPanel.new(
  title='Write Command OPS',
  datasource=common.datasource,
  description='The total count of different kinds of commands received',
  formatY1='ops',
  formatY2='opm',
  min='0',
  fill=0,
  nullPointMode='null as zero',
)
.addTarget(
  prometheus.target(
    'sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_DMWriteBlock{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='write block',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/delete_range|ingest/', yaxis: 2 });

local write_AmplificationP = graphPanel.new(
  title='Write Amplification',
  datasource=common.datasource,
  formatY1='short',
  formatY2='binBps',
  min='0',
  fill=0,
  nullPointMode='null',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'sum by (instance) ( tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} + tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} ) / sum by (instance) ( tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"} )',
    legendFormat='amp-total-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[5m]) )',
    legendFormat='amp-5min-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[10m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[10m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[10m]) )',
    legendFormat='amp-10min-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[30m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[30m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[30m]) )',
    legendFormat='amp-30min-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) )',
    legendFormat='fs-5min-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[5m]) )',
    legendFormat='write-5min-{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addSeriesOverride({ alias: '/fs|write/', yaxis: 2 });

local subTasks_Write_Throughput_bytesP = graphPanel.new(
  title='SubTasks Write Throughput (bytes)',
  datasource=common.datasource,
  description='The throughput of (maybe foreground) tasks of storage in bytes',
  formatY1='binBps',
  formatY2='bytes',
  min='0',
  fill=0,
  nullPointMode='null',
  decimals=1,
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
    'sum(rate(tiflash_storage_subtask_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/total/', yaxis: 2 });

local subTasks_Write_Throughput_rowsP = graphPanel.new(
  title='SubTasks Write Throughput (rows)',
  datasource=common.datasource,
  description='The throughput of (maybe foreground) tasks of storage in rows',
  formatY1='none',
  formatY2='bytes',
  min='0',
  fill=0,
  nullPointMode='null',
  decimals=1,
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
    'sum(rate(tiflash_storage_subtask_throughput_rows{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
    intervalFactor=1,
  )
)
.addSeriesOverride({ alias: '/total/', yaxis: 2 });

local small_Internal_Tasks_OPSP = graphPanel.new(
  title='Small Internal Tasks OPS',
  datasource=common.datasource,
  description='Total number of storage\'s internal sub tasks',
  formatY1='ops',
  formatY2='opm',
  min='0',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_subtask_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (type)',
    legendFormat='{{type}}',
  )
);

local small_Internal_Tasks_DurationP = graphPanel.new(
  title='Small Internal Tasks Duration',
  datasource=common.datasource,
  description='Duration of storage\'s internal sub tasks',
  formatY1='s',
  formatY2='s',
  min='0',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval]))) by (le,type, $additional_groupby) / 1000000000)',
    legendFormat='max-{{type}} {{$additional_groupby}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9999, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
    legendFormat='9999-{{type}} {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
    legendFormat='99-{{type}} {{$additional_groupby}}',
  )
);

local large_Internal_Tasks_OPSP = graphPanel.new(
  title='Large Internal Tasks OPS',
  datasource=common.datasource,
  description='Total number of storage\'s internal sub tasks',
  formatY1='ops',
  formatY2='opm',
  min='0',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_subtask_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (type)',
    legendFormat='{{type}}',
  )
);

local large_Internal_Tasks_DurationP = graphPanel.new(
  title='Large Internal Tasks Duration',
  datasource=common.datasource,
  description='Duration of storage\'s internal sub tasks',
  formatY1='s',
  formatY2='s',
  min='0',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval]))) by (le,type, $additional_groupby) / 1000000000)',
    legendFormat='max-{{type}} {{$additional_groupby}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.9999, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
    legendFormat='9999-{{type}} {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
    legendFormat='99-{{type}} {{$additional_groupby}}',
  )
);

local current_Data_Management_TasksP = graphPanel.new(
  title='Current Data Management Tasks',
  datasource=common.datasource,
  description='The current processing number of  segments\' background management',
  formatY1='short',
  formatY2='none',
  min='0',
  fill=0,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'avg(tiflash_system_current_metric_DT_DeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='delta_merge-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'avg(tiflash_system_current_metric_DT_SegmentSplit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='seg_split-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'avg(tiflash_system_current_metric_DT_SegmentMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='seg_merge-{{instance}}',
    intervalFactor=1,
  )
);

local opened_File_CountP = graphPanel.new(
  title='Opened File Count',
  datasource=common.datasource,
  description='The number of currently opened file descriptors.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
  formatY1='none',
  formatY2='short',
  min='0',
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
    'tiflash_proxy_process_open_fds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
    legendFormat='{{instance}}',
    intervalFactor=1,
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_OpenFileForWrite{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='W-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_OpenFileForRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='R-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_system_current_metric_OpenFileForReadWrite{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
    legendFormat='RW-{{instance}}',
    intervalFactor=1,
  )
);

local file_Open_OPSP = graphPanel.new(
  title='File Open OPS',
  datasource=common.datasource,
  description='The number of open file descriptors action.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
  formatY1='ops',
  formatY2='short',
  min='0',
  fill=0,
  nullPointMode='null',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_hideEmpty=true,
  legend_hideZero=true,
  legend_sort='max',
  legend_sortDesc=true,
  legend_sideWidth=250,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_FileOpen{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='Open-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_FileOpenFailed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='OpenFail-{{instance}}',
    intervalFactor=1,
  )
);

local fSync_StatusP = graphPanel.new(
  title='FSync Status',
  datasource=common.datasource,
  description='OPS and duration of fsync operations.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
  formatY1='ops',
  formatY2='s',
  min='0',
  fill=0,
  nullPointMode='null as zero',
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
    'sum(rate(tiflash_system_profile_event_FileFSync{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance)',
    legendFormat='ops-fsync-{{instance}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_system_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"fsync"}[$__rate_interval]))) by (le, instance) / 1000000000)',
    legendFormat='max-fsync-{{instance}}',
  )
)
.addSeriesOverride({ alias: '/max-fsync/', yaxis: 2 });

local disk_Write_OPSP = graphPanel.new(
  title='Disk Write OPS',
  datasource=common.datasource,
  description='The number of different kinds of read operations',
  formatY1='ops',
  formatY2='none',
  min='0',
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
    'sum(rate(tiflash_system_profile_event_PSMWriteIOCalls{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='Page',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_PSMWritePages{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='PageFile',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWrite{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='File Descriptor',
    intervalFactor=1,
  )
);

local disk_Read_OPSP = graphPanel.new(
  title='Disk Read OPS',
  datasource=common.datasource,
  description='The number of different kinds of read operations',
  formatY1='ops',
  formatY2='none',
  min='0',
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
    'sum(rate(tiflash_system_profile_event_PSMReadIOCalls{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='Page',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_PSMReadPages{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='PageFile',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='File Descriptor',
    intervalFactor=1,
  )
);

local write_flowP = graphPanel.new(
  title='Write flow',
  datasource=common.datasource,
  description='The flow of different kinds of write operations',
  formatY1='binBps',
  formatY2='short',
  min='0',
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
    'sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='File Descriptor',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='Page',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_PSMBackgroundWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='PageBackGround',
    intervalFactor=1,
  )
);

local read_flowP = graphPanel.new(
  title='Read flow',
  datasource=common.datasource,
  description='The flow of different kinds of read operations',
  formatY1='binBps',
  formatY2='short',
  min='0',
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
    'sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='File Descriptor',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_PSMReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='Page',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='PageBackGround',
    intervalFactor=1,
  )
);

local compression_RatioP = graphPanel.new(
  title='Compression Ratio',
  datasource=common.datasource,
  description='The compression ratio of different compression algorithm',
  formatY1='short',
  formatY2='short',
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
    'sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lz4_uncompressed_bytes"}[1m]))/sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lz4_compressed_bytes"}[1m]))',
    legendFormat='lz4',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lightweight_uncompressed_bytes"}[1m]))/sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lightweight_compressed_bytes"}[1m]))',
    legendFormat='lightweight',
  )
);

local compression_Algorithm_CountP = graphPanel.new(
  title='Compression Algorithm Count',
  datasource=common.datasource,
  description='The count of the compression algorithm used by each data part',
  formatY1='short',
  formatY2='short',
  fill=1,
  nullPointMode='null',
  pointradius=2,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_total=true,
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_storage_pack_compression_algorithm_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
  )
);


{
  row: rowObj
  .addPanel(write_Command_OPSP, gridPos=common.pos(12, 8, x=0, y=41))
  .addPanel(write_AmplificationP, gridPos=common.pos(12, 8, x=12, y=41))
  .addPanel(subTasks_Write_Throughput_bytesP, gridPos=common.pos(12, 8, x=0, y=49))
  .addPanel(subTasks_Write_Throughput_rowsP, gridPos=common.pos(12, 8, x=12, y=49))
  .addPanel(small_Internal_Tasks_OPSP, gridPos=common.pos(12, 5, x=0, y=57))
  .addPanel(small_Internal_Tasks_DurationP, gridPos=common.pos(12, 5, x=12, y=57))
  .addPanel(large_Internal_Tasks_OPSP, gridPos=common.pos(12, 5, x=0, y=62))
  .addPanel(large_Internal_Tasks_DurationP, gridPos=common.pos(12, 5, x=12, y=62))
  .addPanel(current_Data_Management_TasksP, gridPos=common.pos(24, 7, x=0, y=67))
  .addPanel(opened_File_CountP, gridPos=common.pos(8, 7, x=0, y=74))
  .addPanel(file_Open_OPSP, gridPos=common.pos(8, 7, x=8, y=74))
  .addPanel(fSync_StatusP, gridPos=common.pos(8, 7, x=16, y=74))
  .addPanel(disk_Write_OPSP, gridPos=common.pos(12, 7, x=0, y=81))
  .addPanel(disk_Read_OPSP, gridPos=common.pos(12, 7, x=12, y=81))
  .addPanel(write_flowP, gridPos=common.pos(12, 8, x=0, y=88))
  .addPanel(read_flowP, gridPos=common.pos(12, 8, x=12, y=88))
  .addPanel(compression_RatioP, gridPos=common.pos(12, 7, x=0, y=96))
  .addPanel(compression_Algorithm_CountP, gridPos=common.pos(12, 7, x=12, y=96))
  ,
  panels: [
    { panel: write_Command_OPSP, w: 12, h: 8, x: 0, y: 41 },
    { panel: write_AmplificationP, w: 12, h: 8, x: 12, y: 41 },
    { panel: subTasks_Write_Throughput_bytesP, w: 12, h: 8, x: 0, y: 49 },
    { panel: subTasks_Write_Throughput_rowsP, w: 12, h: 8, x: 12, y: 49 },
    { panel: small_Internal_Tasks_OPSP, w: 12, h: 5, x: 0, y: 57 },
    { panel: small_Internal_Tasks_DurationP, w: 12, h: 5, x: 12, y: 57 },
    { panel: large_Internal_Tasks_OPSP, w: 12, h: 5, x: 0, y: 62 },
    { panel: large_Internal_Tasks_DurationP, w: 12, h: 5, x: 12, y: 62 },
    { panel: current_Data_Management_TasksP, w: 24, h: 7, x: 0, y: 67 },
    { panel: opened_File_CountP, w: 8, h: 7, x: 0, y: 74 },
    { panel: file_Open_OPSP, w: 8, h: 7, x: 8, y: 74 },
    { panel: fSync_StatusP, w: 8, h: 7, x: 16, y: 74 },
    { panel: disk_Write_OPSP, w: 12, h: 7, x: 0, y: 81 },
    { panel: disk_Read_OPSP, w: 12, h: 7, x: 12, y: 81 },
    { panel: write_flowP, w: 12, h: 8, x: 0, y: 88 },
    { panel: read_flowP, w: 12, h: 8, x: 12, y: 88 },
    { panel: compression_RatioP, w: 12, h: 7, x: 0, y: 96 },
    { panel: compression_Algorithm_CountP, w: 12, h: 7, x: 12, y: 96 }
  ],
}
