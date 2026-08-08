// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='S3');

local panelS3Bytes = graphPanel.new(
  title='S3 Bytes',
  datasource=common.datasource,
  description='S3 read/write throughput',
  fill=0,
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
    'sum(rate(tiflash_system_profile_event_S3WriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3WriteBytes {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3ReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3ReadBytes {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3WriteDMFileBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3WriteDMFileBytes {{$additional_groupby}}',
  )
)
.resetYaxes()
.addYaxis(
  format='binBps',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
);

local panelS3Ops = graphPanel.new(
  title='S3 OPS',
  datasource=common.datasource,
  description='S3 OPS',
  fill=0,
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
    'sum(rate(tiflash_system_profile_event_S3PutObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3PutObject {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3GetObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3GetObject {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3HeadObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3HeadObject {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3ListObjects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3ListObjects {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3DeleteObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3DeleteObject {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3CopyObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3CopyObject {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3CreateMultipartUpload{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3CreateMultipartUpload {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3UploadPart{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3UploadPart {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3CompleteMultipartUpload{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3CompleteMultipartUpload {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3PutDMFile{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3PutDMFile {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3IORead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IORead {{$additional_groupby}}',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3IOSeek{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IOSeek {{$additional_groupby}}',
    hide=true,
  )
)
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
);

local panelS3RetryOps = graphPanel.new(
  title='S3 Retry OPS',
  datasource=common.datasource,
  description='S3 Retry OPS',
  fill=0,
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
    'sum(rate(tiflash_system_profile_event_S3GetObjectRetry{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3GetObjectRetry {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3PutObjectRetry{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3PutObjectRetry {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3PutDMFileRetry{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3PutDMFileRetry {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3IOReadError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IOReadError {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3IOSeekError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IOSeekError {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3IOSeekBackward{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IOSeekBackward {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
);

local panelS3RequestDuration = common.durationPanel(
  'S3 Request Duration',
  'tiflash_storage_s3_request_seconds_bucket',
  by=['type'],
  legend='{{type}}-%s {{$additional_groupby}}',
  description='S3 Request Duration',
);

local panelS3HttpOps = graphPanel.new(
  title='S3 HTTP OPS',
  datasource=common.datasource,
  description='S3 HTTP OPS',
  fill=0,
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
    'sum(rate(tiflash_system_profile_event_S3ReadRequestsCount{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='read-count {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3WriteRequestsCount{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='write-count {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3ReadRequestsErrors{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='read-error {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3WriteRequestsErrors{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='write-error {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3ReadRequestsThrottling{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='read-throttling {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3WriteRequestsThrottling{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='write-throttling {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3ReadRequestsRedirects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='read-redirects {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3WriteRequestsRedirects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='write-redirects {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3ReadRequestsNotFound{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='read-notfound {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3WriteRequestsNotFound{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='write-notfound {{$additional_groupby}}',
  )
)
.resetYaxes()
.addYaxis(
  format='ops',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
);

local panelS3HttpRequestDuration = common.durationPanel(
  'S3 HTTP Request Duration',
  'tiflash_storage_s3_http_request_seconds_bucket',
  by=['type'],
  legend='{{type}}-%s {{$additional_groupby}}',
  description='S3 HTTP Request Duration',
);

local panelS3OnGoingInstances = graphPanel.new(
  title='S3 on-going instances',
  datasource=common.datasource,
  description='S3 HTTP OPS',
  fill=0,
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
    'sum by (type, $additional_groupby) (tiflash_system_current_metric_S3Requests{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
    legendFormat='S3Requests {{$additional_groupby}}',
  )
)
.addTarget(
  prometheus.target(
    'sum by (type, $additional_groupby) (tiflash_system_current_metric_S3RandomAccessFile{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
    legendFormat='S3RandomAccessFile {{$additional_groupby}}',
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  min='0',
)
.addYaxis(
  format='opm',
  min='0',
);

local panelS3randomaccessfileOps = graphPanel.new(
  title='S3RandomAccessFile OPS',
  datasource=common.datasource,
  fill=0,
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
    'sum(rate(tiflash_system_profile_event_S3IOReadError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IOReadError {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3IOSeekError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IOSeekError {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3IOSeekBackward{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IOSeekBackward {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3IORead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IORead {{$additional_groupby}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_system_profile_event_S3IOSeek{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type, $additional_groupby)',
    legendFormat='S3IOSeek {{$additional_groupby}}',
    intervalFactor=1,
  )
)
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
      common.band([panelS3Bytes, panelS3Ops]),
      common.band([panelS3RetryOps, panelS3RequestDuration]),
      common.band([panelS3HttpOps, panelS3HttpRequestDuration]),
      common.band([panelS3OnGoingInstances, panelS3randomaccessfileOps])
    ],
  ),
}
