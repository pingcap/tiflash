// Generated from tiflash_summary.json — edit carefully or regenerate.
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Task Scheduler');

local min_TSOP = graphPanel.new(
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

local estimated_Thread_Usage_and_LimitP = graphPanel.new(
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

local active_and_Waiting_Queries_CountP = graphPanel.new(
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

local active_and_Waiting_Tasks_CountP = graphPanel.new(
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

local hard_Limit_Exceeded_CountP = graphPanel.new(
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

local task_Waiting_DurationP = graphPanel.new(
  title='Task Waiting Duration',
  datasource=common.datasource,
  description='the time of waiting for schedule',
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
    'histogram_quantile(0.80, max(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,le,resource_group))',
    legendFormat='{{instance}}-{{resource_group}}-80',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.90, max(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,le,resource_group))',
    legendFormat='{{instance}}-{{resource_group}}-90',
    hide=true,
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.00, max(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (instance,le,resource_group))',
    legendFormat='{{instance}}-{{resource_group}}-100',
  )
)
.resetYaxes()
.addYaxis(
  format='s',
  label='Time',
)
.addYaxis(
  format='short',
);


{
  row: rowObj
  .addPanel(min_TSOP, gridPos=common.pos(12, 8, x=0, y=37))
  .addPanel(estimated_Thread_Usage_and_LimitP, gridPos=common.pos(12, 8, x=12, y=37))
  .addPanel(active_and_Waiting_Queries_CountP, gridPos=common.pos(12, 8, x=0, y=45))
  .addPanel(active_and_Waiting_Tasks_CountP, gridPos=common.pos(12, 8, x=12, y=45))
  .addPanel(hard_Limit_Exceeded_CountP, gridPos=common.pos(12, 8, x=0, y=53))
  .addPanel(task_Waiting_DurationP, gridPos=common.pos(12, 8, x=12, y=53))
  ,
  panels: [
    { panel: min_TSOP, w: 12, h: 8, x: 0, y: 37 },
    { panel: estimated_Thread_Usage_and_LimitP, w: 12, h: 8, x: 12, y: 37 },
    { panel: active_and_Waiting_Queries_CountP, w: 12, h: 8, x: 0, y: 45 },
    { panel: active_and_Waiting_Tasks_CountP, w: 12, h: 8, x: 12, y: 45 },
    { panel: hard_Limit_Exceeded_CountP, w: 12, h: 8, x: 0, y: 53 },
    { panel: task_Waiting_DurationP, w: 12, h: 8, x: 12, y: 53 }
  ],
}
