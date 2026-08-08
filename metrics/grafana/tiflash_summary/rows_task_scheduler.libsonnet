// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Task Scheduler');

local panelMinTso = graphPanel.new(
  title='Min TSO',
  datasource=common.datasource,
  description='the min_tso of each instance',
  fill=1,
  nullPointMode='null',
  points=true,
  lines=false,
  pointradius=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="min_tso"}) by (instance, resource_group)',
    legendFormat='{{instance}}-{{resource_group}}',
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  label='TSO',
  show=false,
)
.addYaxis(
  format='short',
);

local panelEstimatedThreadUsageAndLimit = graphPanel.new(
  title='Estimated Thread Usage and Limit',
  datasource=common.datasource,
  description='estimated thread usage in min-tso scheduler, and the sort/hard limit of estimated thread in scheduler.',
  fill=0,
  nullPointMode='null as zero',
  pointradius=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
)
.addTarget(
  prometheus.target(
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="thread_soft_limit"}) by (instance, type, resource_group)',
    legendFormat='{{instance}}-{{type}}-{{resource_group}}',
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="estimated_thread_usage"}) by (instance, type, resource_group)',
    legendFormat='{{instance}}-{{type}}-{{resource_group}}',
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="thread_hard_limit"}) by (instance, type, resource_group)',
    legendFormat='{{instance}}-{{type}}-{{resource_group}}',
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="global_estimated_thread_usage"}) by (instance, type, resource_group)',
    legendFormat='{{instance}}-{{type}}-{{resource_group}}',
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="group_entry_count"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  label='Threads',
  logBase=10,
)
.addYaxis(
  format='short',
);

local panelActiveAndWaitingQueriesCount = graphPanel.new(
  title='Active and Waiting Queries Count',
  datasource=common.datasource,
  description='the count of active/ waiting queries',
  fill=0,
  nullPointMode='null as zero',
  pointradius=1,
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
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="waiting_queries_count"}) by (instance, type, resource_group)',
    legendFormat='{{instance}}-{{type}}-{{resource_group}}',
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="active_queries_count"}) by (instance, type, resource_group)',
    legendFormat='{{instance}}-{{type}}-{{resource_group}}',
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  label='Queries',
)
.addYaxis(
  format='short',
);

local panelActiveAndWaitingTasksCount = graphPanel.new(
  title='Active and Waiting Tasks Count',
  datasource=common.datasource,
  description='the count of active/ waiting tasks',
  fill=0,
  nullPointMode='null as zero',
  pointradius=1,
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
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="waiting_tasks_count"}) by (instance, type, resource_group)',
    legendFormat='{{instance}}-{{type}}-{{resource_group}}',
  )
)
.addTarget(
  prometheus.target(
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="active_tasks_count"}) by (instance, type, resource_group)',
    legendFormat='{{instance}}-{{type}}-{{resource_group}}',
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  label='Tasks',
)
.addYaxis(
  format='short',
);

local panelHardLimitExceededCount = graphPanel.new(
  title='Hard Limit Exceeded Count',
  datasource=common.datasource,
  description='the usage of estimated threads exceeded the hard limit where errors occur.',
  fill=0,
  nullPointMode='null as zero',
  pointradius=1,
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
)
.addTarget(
  prometheus.target(
    'max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="hard_limit_exceeded_count"}) by (instance, type, resource_group)',
    legendFormat='{{instance}}-{{resource_group}}',
  )
)
.resetYaxes()
.addYaxis(
  format='none',
)
.addYaxis(
  format='short',
);

local panelTaskWaitingDuration = common.durationPanel(
  'Task Waiting Duration',
  'tiflash_task_scheduler_waiting_duration_seconds_bucket',
  by=['instance', 'resource_group'],
  legend='{{instance}}-{{resource_group}}-%s',
  description='the time of waiting for schedule',
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([panelMinTso, panelEstimatedThreadUsageAndLimit]),
      common.band([panelActiveAndWaitingQueriesCount, panelActiveAndWaitingTasksCount]),
      common.band([panelHardLimitExceededCount, panelTaskWaitingDuration])
    ],
  ),
}
