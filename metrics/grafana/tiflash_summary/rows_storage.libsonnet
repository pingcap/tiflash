// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Storage');

local panelWriteCommandOps = graphPanel.new(
  title='Write Command OPS',
  datasource=common.datasource,
  description='The total count of different kinds of commands received',
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
.addSeriesOverride({ alias: '/delete_range|ingest/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
);

local panelWriteAmplification = graphPanel.new(
  title='Write Amplification',
  datasource=common.datasource,
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
.addSeriesOverride({ alias: '/fs|write/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
  max='20',
)
.addYaxis(
  format='binBps',
);

local panelSubtasksWriteThroughputBytes = graphPanel.new(
  title='SubTasks Write Throughput (bytes)',
  datasource=common.datasource,
  description='The throughput of (maybe foreground) tasks of storage in bytes',
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
.addSeriesOverride({ alias: '/total/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
)
.addYaxis(
  format='bytes',
  show=false,
);

local panelSubtasksWriteThroughputRows = graphPanel.new(
  title='SubTasks Write Throughput (rows)',
  datasource=common.datasource,
  description='The throughput of (maybe foreground) tasks of storage in rows',
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
.addSeriesOverride({ alias: '/total/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='none',
  min='0',
)
.addYaxis(
  format='bytes',
  show=false,
);

local panelSmallInternalTasksOps = common.opsPanel(
  'Small Internal Tasks OPS',
  'tiflash_storage_subtask_count',
  by=['type'],
  labels='type!~"(delta_merge|seg_merge|seg_split).*"',
  description="Total number of storage's internal sub tasks",
  yRight='opm',
);

local panelSmallInternalTasksDuration = common.durationPanel(
  'Small Internal Tasks Duration',
  'tiflash_storage_subtask_duration_seconds_bucket',
  selector=common.selector + ', type!~"(delta_merge|seg_merge|seg_split).*"',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
  description="Duration of storage's internal sub tasks",
);

local panelLargeInternalTasksOps = common.opsPanel(
  'Large Internal Tasks OPS',
  'tiflash_storage_subtask_count',
  by=['type'],
  labels='type=~"(delta_merge|seg_merge|seg_split).*"',
  description="Total number of storage's internal sub tasks",
  yRight='opm',
);

local panelLargeInternalTasksDuration = common.durationPanel(
  'Large Internal Tasks Duration',
  'tiflash_storage_subtask_duration_seconds_bucket',
  selector=common.selector + ', type=~"(delta_merge|seg_merge|seg_split).*"',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
  description="Duration of storage's internal sub tasks",
);

local panelCurrentDataManagementTasks = graphPanel.new(
  title='Current Data Management Tasks',
  datasource=common.datasource,
  description='The current processing number of  segments\' background management',
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
)
.resetYaxes()
.addYaxis(
  format='short',
  min='0',
  decimals=0,
)
.addYaxis(
  format='none',
);

local panelOpenedFileCount = graphPanel.new(
  title='Opened File Count',
  datasource=common.datasource,
  description='The number of currently opened file descriptors.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
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

local panelFileOpenOps = graphPanel.new(
  title='File Open OPS',
  datasource=common.datasource,
  description='The number of open file descriptors action.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
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
)
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='short',
  show=false,
);

local panelFsyncStatus = graphPanel.new(
  title='FSync Status',
  datasource=common.datasource,
  description='OPS and duration of fsync operations.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)',
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
.addSeriesOverride({ alias: '/max-fsync/', yaxis: 2 })
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='s',
);

local panelDiskWriteOps = graphPanel.new(
  title='Disk Write OPS',
  datasource=common.datasource,
  description='The number of different kinds of read operations',
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
)
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='none',
);

local panelDiskReadOps = graphPanel.new(
  title='Disk Read OPS',
  datasource=common.datasource,
  description='The number of different kinds of read operations',
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
)
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='none',
);

local panelWriteFlow = graphPanel.new(
  title='Write flow',
  datasource=common.datasource,
  description='The flow of different kinds of write operations',
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
)
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
)
.addYaxis(
  format='short',
  min='0',
);

local panelReadFlow = graphPanel.new(
  title='Read flow',
  datasource=common.datasource,
  description='The flow of different kinds of read operations',
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
)
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
)
.addYaxis(
  format='short',
  min='0',
);

local panelCompressionRatio = graphPanel.new(
  title='Compression Ratio',
  datasource=common.datasource,
  description='The compression ratio of different compression algorithm',
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
)
.resetYaxes()
.addYaxis(
  format='short',
)
.addYaxis(
  format='short',
);

local panelCompressionAlgorithmCount = graphPanel.new(
  title='Compression Algorithm Count',
  datasource=common.datasource,
  description='The count of the compression algorithm used by each data part',
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
)
.resetYaxes()
.addYaxis(
  format='short',
)
.addYaxis(
  format='short',
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelWriteCommandOps, panelWriteAmplification]),
      common.band([panelSubtasksWriteThroughputBytes, panelSubtasksWriteThroughputRows]),
      common.band([panelSmallInternalTasksOps, panelSmallInternalTasksDuration], h=5),
      common.band([panelLargeInternalTasksOps, panelLargeInternalTasksDuration], h=5),
      common.band([panelCurrentDataManagementTasks]),
      common.band([panelOpenedFileCount, panelFileOpenOps, panelFsyncStatus]),
      common.band([panelDiskWriteOps, panelDiskReadOps]),
      common.band([panelWriteFlow, panelReadFlow]),
      common.band([panelCompressionRatio, panelCompressionAlgorithmCount])
    ],
  ),
}
