// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

local grafana = import 'grafonnet/grafana.libsonnet';
local dashboard = grafana.dashboard;
local template = grafana.template;
local common = import 'tiflash_summary/common.libsonnet';

local row_server = import 'tiflash_summary/rows_server.libsonnet';
local row_threads_cpu = import 'tiflash_summary/rows_threads_cpu.libsonnet';
local row_threads = import 'tiflash_summary/rows_threads.libsonnet';
local row_coprocessor = import 'tiflash_summary/rows_coprocessor.libsonnet';
local row_task_scheduler = import 'tiflash_summary/rows_task_scheduler.libsonnet';
local row_ddl = import 'tiflash_summary/rows_ddl.libsonnet';
local row_imbalance_read_write = import 'tiflash_summary/rows_imbalance.libsonnet';
local row_memory_trace = import 'tiflash_summary/rows_memory_trace.libsonnet';
local row_columnar_storage = import 'tiflash_summary/rows_columnar_storage.libsonnet';
local row_storage = import 'tiflash_summary/rows_storage.libsonnet';
local row_storage_read_pool_data_sharing = import 'tiflash_summary/rows_storage_read_pool.libsonnet';
local row_pagestorage = import 'tiflash_summary/rows_pagestorage.libsonnet';
local row_rate_limiter = import 'tiflash_summary/rows_rate_limiter.libsonnet';
local row_storage_write_stall = import 'tiflash_summary/rows_storage_write_stall.libsonnet';
local row_raft = import 'tiflash_summary/rows_raft.libsonnet';
local row_raft_snapshot_ingestsst = import 'tiflash_summary/rows_raft_snapshot.libsonnet';
local row_rough_set_filter_rate_histogram = import 'tiflash_summary/rows_rough_set.libsonnet';
local row_disaggregated_write = import 'tiflash_summary/rows_disagg_write.libsonnet';
local row_disaggregated_compute = import 'tiflash_summary/rows_disagg_compute.libsonnet';
local row_s3 = import 'tiflash_summary/rows_s3.libsonnet';
local row_pipeline_model = import 'tiflash_summary/rows_pipeline_model.libsonnet';
local row_tiflash_resource_control = import 'tiflash_summary/rows_resource_control.libsonnet';
local row_status_server = import 'tiflash_summary/rows_status_server.libsonnet';
local row_vector_search = import 'tiflash_summary/rows_vector_search.libsonnet';

local myNameFlag = 'DS_TEST-CLUSTER';

dashboard.new(
  title='Test-Cluster-TiFlash-Summary',
  uid='SVbh2xUWk',
  editable=true,
  graphTooltip='shared_crosshair',
  refresh='1m',
  time_from='now-1h',
  schemaVersion=27,
  style='dark',
)
.addInput(
  name=myNameFlag,
  label='Test-Cluster',
  type='datasource',
  pluginId='prometheus',
  pluginName='Prometheus',
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    hide='all',
    label='K8s-cluster',
    name='k8s_cluster',
    query='label_values(tiflash_system_profile_event_Query, k8s_cluster)',
    refresh='time',
    sort=1,
  )
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    hide='all',
    includeAll=false,
    label='tidb_cluster',
    multi=false,
    name='tidb_cluster',
    query='label_values(tiflash_system_profile_event_Query{k8s_cluster="$k8s_cluster"}, tidb_cluster)',
    refresh='time',
    sort=1,
  )
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    includeAll=true,
    label='Instance',
    multi=true,
    name='instance',
    query='label_values(tiflash_system_profile_event_Query{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
    refresh='load',
    sort=1,
  )
)
.addTemplate(
  template.new(
    datasource=common.datasource,
    includeAll=true,
    label='Proxy Instance',
    multi=true,
    name='proxy_instance',
    query='label_values(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
    refresh='load',
    sort=1,
  )
)
.addTemplate(
  template.custom(
    name='additional_groupby',
    query='none,instance',
    current='none',
    label='additional_groupby',
  )
)
.addTemplate(
  template.custom(
    name='tiflash_role',
    query='.*,.*write-tiflash.*,.*compute-tiflash.*',
    current='.*',
    label='Role',
    valuelabels={
      '.*': 'All',
      '.*write-tiflash.*': 'Write',
      '.*compute-tiflash.*': 'Compute',
    },
  )
)
.addPanel(row_server.row, gridPos=common.rowPos)
.addPanel(row_threads_cpu.row, gridPos=common.rowPos)
.addPanel(row_threads.row, gridPos=common.rowPos)
.addPanel(row_coprocessor.row, gridPos=common.rowPos)
.addPanel(row_task_scheduler.row, gridPos=common.rowPos)
.addPanel(row_ddl.row, gridPos=common.rowPos)
.addPanel(row_imbalance_read_write.row, gridPos=common.rowPos)
.addPanel(row_memory_trace.row, gridPos=common.rowPos)
.addPanel(row_columnar_storage.row, gridPos=common.rowPos)
.addPanel(row_storage.row, gridPos=common.rowPos)
.addPanel(row_storage_read_pool_data_sharing.row, gridPos=common.rowPos)
.addPanel(row_pagestorage.row, gridPos=common.rowPos)
.addPanel(row_rate_limiter.row, gridPos=common.rowPos)
.addPanel(row_storage_write_stall.row, gridPos=common.rowPos)
.addPanel(row_raft.row, gridPos=common.rowPos)
.addPanel(row_raft_snapshot_ingestsst.row, gridPos=common.rowPos)
.addPanel(row_rough_set_filter_rate_histogram.row, gridPos=common.rowPos)
.addPanel(row_disaggregated_write.row, gridPos=common.rowPos)
.addPanel(row_disaggregated_compute.row, gridPos=common.rowPos)
.addPanel(row_s3.row, gridPos=common.rowPos)
.addPanel(row_pipeline_model.row, gridPos=common.rowPos)
.addPanel(row_tiflash_resource_control.row, gridPos=common.rowPos)
.addPanel(row_status_server.row, gridPos=common.rowPos)
.addPanel(row_vector_search.row, gridPos=common.rowPos)
