// Generated from tiflash_summary.json — edit carefully or regenerate.
// Layout: use common.band / common.buildRow (do not hand-write x/y/w).
local grafana = import 'grafonnet/grafana.libsonnet';
local row = grafana.row;
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local common = import 'common.libsonnet';

local rowObj = row.new(collapse=true, title='Pipeline Model');

local task_Thread_Pool_SizeP = graphPanel.new(
  title='Task Thread Pool Size',
  datasource=common.datasource,
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
    'max(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_task_thread_pool_size"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
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
);

local task_CountP = graphPanel.new(
  title='Task Count',
  datasource=common.datasource,
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
    'max(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_tasks_count"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_tasks_count"}) by (type)',
    legendFormat='sum({{type}})',
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  min='0',
)
.addYaxis(
  format='short',
);

local task_Status_Change_OPSP = common.opsPanel(
  'Task Status Change OPS',
  'tiflash_pipeline_task_change_to_status',
  by=['type'],
  yLeft='none',
  yRight='short',
);

local task_DurationP = common.durationPanel(
  'Task Duration',
  'tiflash_pipeline_task_duration_seconds_bucket',
  by=['type'],
  legend='%s-{{type}} {{$additional_groupby}}',
  extraTargets=[
    common.target(
      '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="cpu_execute"')
      + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="cpu_execute"') + ')',
      'avg-cpu_execute',
    ),
    common.target(
      '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="cpu_queue"')
      + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="cpu_queue"') + ')',
      'avg-cpu_queue',
    ),
    common.target(
      '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="io_execute"')
      + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="io_execute"') + ')',
      'avg-io_execute',
    ),
    common.target(
      '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="io_queue"')
      + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="io_queue"') + ')',
      'avg-io_queue',
    ),
    common.target(
      '(' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_sum', common.selector, labels='type="await"')
      + ' / ' + common.expr.sumRate('tiflash_pipeline_task_duration_seconds_count', common.selector, labels='type="await"') + ')',
      'avg-await',
    ),
  ],
);

local task_Max_Execute_Time_Per_RoundP = graphPanel.new(
  title='Task Max Execute Time Per Round',
  datasource=common.datasource,
  fill=1,
  nullPointMode='null as zero',
  legend_alignAsTable=true,
  legend_rightSide=true,
  legend_values=true,
  legend_current=true,
  legend_max=true,
  legend_sort='current',
  legend_sortDesc=true,
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m]))) by (le, type) / 1000000000)',
    legendFormat='100-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu"}[1m])) / sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu"}[1m]))',
    legendFormat='avg-cpu',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io"}[1m])) / sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io"}[1m]))',
    legendFormat='avg-io',
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

local threads_CPU_of_CPU_Task_Thread_PoolP = common.cpuWithLimitPanel(
  'Threads CPU of CPU Task Thread Pool',
  'cpu_pool',
  legend='{{name}} {{instance}}',
);

local threads_CPU_of_IO_Task_Thread_PoolP = common.cpuWithLimitPanel(
  'Threads CPU of IO Task Thread Pool',
  'io_pool',
  legend='{{name}} {{instance}}',
);

local threads_CPU_of_Wait_ReactorP = common.cpuWithLimitPanel(
  'Threads CPU of Wait Reactor',
  'WaitReactor',
  legend='{{name}} {{instance}}',
);

local wait_notify_task_detailsP = graphPanel.new(
  title='Wait notify task details',
  datasource=common.datasource,
  description='wait notify task details',
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
    'max(tiflash_pipeline_wait_on_notify_tasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
    legendFormat='{{instance}}-{{type}}',
    intervalFactor=1,
  )
)
.addTarget(
  prometheus.target(
    'sum(tiflash_pipeline_wait_on_notify_tasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type)',
    legendFormat='sum({{type}})',
  )
)
.resetYaxes()
.addYaxis(
  format='none',
  min='0',
)
.addYaxis(
  format='short',
);


{
  row: common.buildRow(
    rowObj,
    [
      common.band([task_Thread_Pool_SizeP, task_CountP]),
      common.band([task_Status_Change_OPSP, task_DurationP]),
      common.band([task_Max_Execute_Time_Per_RoundP, threads_CPU_of_CPU_Task_Thread_PoolP]),
      common.band([threads_CPU_of_IO_Task_Thread_PoolP, threads_CPU_of_Wait_ReactorP]),
      common.band([{ panel: wait_notify_task_detailsP, w: 12 }])
    ],
  ),
}
