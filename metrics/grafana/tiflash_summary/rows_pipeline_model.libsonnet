// Generated from tiflash_summary.json — edit carefully or regenerate.
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

local task_Status_Change_OPSP = graphPanel.new(
  title='Task Status Change OPS',
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
    'sum(rate(tiflash_pipeline_task_change_to_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (type)',
    legendFormat='{{type}}',
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

local task_DurationP = graphPanel.new(
  title='Task Duration',
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
    'histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='95-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='99-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'histogram_quantile(0.99, sum(rate(tiflash_pipeline_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[1m])) by (le, type))',
    legendFormat='999-{{type}}',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu_execute"}[1m])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu_execute"}[1m]))',
    legendFormat='avg-cpu_execute',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu_queue"}[1m])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu_queue"}[1m]))',
    legendFormat='avg-cpu_queue',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io_execute"}[1m])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io_execute"}[1m]))',
    legendFormat='avg-io_execute',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io_queue"}[1m])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io_queue"}[1m]))',
    legendFormat='avg-io_queue',
  )
)
.addTarget(
  prometheus.target(
    'sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="await"}[1m])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="await"}[1m]))',
    legendFormat='avg-await',
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

local threads_CPU_of_CPU_Task_Thread_PoolP = graphPanel.new(
  title='Threads CPU of CPU Task Thread Pool',
  datasource=common.datasource,
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"cpu_pool", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"cpu_pool", instance=~"$proxy_instance", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
.resetYaxes()
.addYaxis(
  format='percentunit',
  min='0',
  decimals=1,
)
.addYaxis(
  format='short',
  show=false,
);

local threads_CPU_of_IO_Task_Thread_PoolP = graphPanel.new(
  title='Threads CPU of IO Task Thread Pool',
  datasource=common.datasource,
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"io_pool", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"io_pool", instance=~"$proxy_instance", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
.resetYaxes()
.addYaxis(
  format='percentunit',
  min='0',
  decimals=1,
)
.addYaxis(
  format='short',
  show=false,
);

local threads_CPU_of_Wait_ReactorP = graphPanel.new(
  title='Threads CPU of Wait Reactor',
  datasource=common.datasource,
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
    'sum by (instance) (rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"WaitReactor", instance=~"$proxy_instance", instance=~"$tiflash_role"}[1m]))',
    legendFormat='{{name}} {{instance}}',
  )
)
.addTarget(
  prometheus.target(
    'count by (instance) (tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"WaitReactor", instance=~"$proxy_instance", instance=~"$tiflash_role"})',
    legendFormat='Limit',
  )
)
.addSeriesOverride({ alias: 'Limit', color: '#F2495C', hideTooltip: true, legend: false, linewidth: 2, nullPointMode: 'connected' })
.resetYaxes()
.addYaxis(
  format='percentunit',
  min='0',
  decimals=1,
)
.addYaxis(
  format='short',
  show=false,
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
  row: rowObj
  .addPanel(task_Thread_Pool_SizeP, gridPos=common.pos(12, 8, x=0, y=162))
  .addPanel(task_CountP, gridPos=common.pos(12, 8, x=12, y=162))
  .addPanel(task_Status_Change_OPSP, gridPos=common.pos(12, 8, x=0, y=170))
  .addPanel(task_DurationP, gridPos=common.pos(12, 8, x=12, y=170))
  .addPanel(task_Max_Execute_Time_Per_RoundP, gridPos=common.pos(12, 8, x=0, y=178))
  .addPanel(threads_CPU_of_CPU_Task_Thread_PoolP, gridPos=common.pos(12, 8, x=12, y=178))
  .addPanel(threads_CPU_of_IO_Task_Thread_PoolP, gridPos=common.pos(12, 8, x=0, y=186))
  .addPanel(threads_CPU_of_Wait_ReactorP, gridPos=common.pos(12, 8, x=12, y=186))
  .addPanel(wait_notify_task_detailsP, gridPos=common.pos(12, 8, x=0, y=194))
  ,
  panels: [
    { panel: task_Thread_Pool_SizeP, w: 12, h: 8, x: 0, y: 162 },
    { panel: task_CountP, w: 12, h: 8, x: 12, y: 162 },
    { panel: task_Status_Change_OPSP, w: 12, h: 8, x: 0, y: 170 },
    { panel: task_DurationP, w: 12, h: 8, x: 12, y: 170 },
    { panel: task_Max_Execute_Time_Per_RoundP, w: 12, h: 8, x: 0, y: 178 },
    { panel: threads_CPU_of_CPU_Task_Thread_PoolP, w: 12, h: 8, x: 12, y: 178 },
    { panel: threads_CPU_of_IO_Task_Thread_PoolP, w: 12, h: 8, x: 0, y: 186 },
    { panel: threads_CPU_of_Wait_ReactorP, w: 12, h: 8, x: 12, y: 186 },
    { panel: wait_notify_task_detailsP, w: 12, h: 8, x: 0, y: 194 }
  ],
}
