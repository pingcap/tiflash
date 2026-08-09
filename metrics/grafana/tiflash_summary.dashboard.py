# Generated from tiflash_summary.json — prefer editing with common.py helpers.
import os
import sys

sys.path.append(os.path.dirname(__file__))

from grafanalib.core import (
    GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
    HIDE_VARIABLE,
    SHOW,
    Dashboard,
    RowPanel,
    Template,
    Templating,
)

from common import (
    DATASOURCE,
    DATASOURCE_INPUT,
    Layout,
    graph_legend,
    graph_panel,
    make_heatmap,
    target,
    template,
    yaxes,
)


def Templates() -> Templating:
    return Templating(
        list=[
            template(
                name="k8s_cluster",
                type="query",
                query="label_values(tiflash_system_profile_event_Query, k8s_cluster)",
                data_source=DATASOURCE,
                hide=HIDE_VARIABLE,
                multi=False,
                include_all=False,
                all_value=None,
                label="K8s-cluster",
                refresh=2,
            ),
            template(
                name="tidb_cluster",
                type="query",
                query='label_values(tiflash_system_profile_event_Query{k8s_cluster="$k8s_cluster"}, tidb_cluster)',
                data_source=DATASOURCE,
                hide=HIDE_VARIABLE,
                multi=False,
                include_all=False,
                all_value=None,
                label="tidb_cluster",
                refresh=2,
            ),
            template(
                name="instance",
                type="query",
                query='label_values(tiflash_system_profile_event_Query{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
                data_source=DATASOURCE,
                hide=SHOW,
                multi=True,
                include_all=True,
                all_value=None,
                label="Instance",
                refresh=1,
            ),
            template(
                name="proxy_instance",
                type="query",
                query='label_values(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster"}, instance)',
                data_source=DATASOURCE,
                hide=SHOW,
                multi=True,
                include_all=True,
                all_value=None,
                label="Proxy Instance",
                refresh=1,
            ),
            # Custom vars: pass explicit options so text/value/current match clinic JSON.
            # grafanalib's comma-split ignores "All : .*" label:value syntax.
            Template(
                name="additional_groupby",
                type="custom",
                query="none,instance",
                dataSource=None,
                hide=SHOW,
                label="additional_groupby",
                default="none",
                options=[
                    {"selected": True, "text": "none", "value": "none"},
                    {"selected": False, "text": "instance", "value": "instance"},
                ],
            ),
            Template(
                name="tiflash_role",
                type="custom",
                query="All : .*, Write : .*write-tiflash.*, Compute : .*compute-tiflash.*",
                dataSource=None,
                hide=SHOW,
                label="Role",
                default=".*",
                options=[
                    {"selected": True, "text": "All", "value": ".*"},
                    {
                        "selected": False,
                        "text": "Write",
                        "value": ".*write-tiflash.*",
                    },
                    {
                        "selected": False,
                        "text": "Compute",
                        "value": ".*compute-tiflash.*",
                    },
                ],
            ),
        ]
    )


def Server() -> RowPanel:
    layout = Layout(title="Server")
    layout.row(
        [
            graph_panel(
                title="Store size",
                description="The storage size per TiFlash instance.\n(Not including some disk usage of TiFlash-Proxy by now)",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_StoreSizeUsed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~""}) by (instance)',
                        legend_format="{{instance}}-local",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_StoreSizeUsedRemote{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="{{instance}}-remote",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                legend=graph_legend(max=False),
                fill=5,
                fill_gradient=0,
                line_width=0,
                stack=True,
                decimals=3,
            ),
            graph_panel(
                title="Available size",
                description="The available capacity size per TiFlash instance",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_StoreSizeAvailable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                legend=graph_legend(max=False),
                fill=5,
                fill_gradient=0,
                line_width=0,
                stack=True,
                decimals=3,
            ),
            graph_panel(
                title="Capacity size",
                description="The capacity size per TiFlash instance",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_StoreSizeCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                legend=graph_legend(max=False),
                fill=5,
                fill_gradient=0,
                line_width=0,
                stack=True,
                decimals=3,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Uptime",
                description="TiFlash uptime since last restart",
                targets=[
                    target(
                        expr='tiflash_system_asynchronous_metric_Uptime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="dtdurations", right_format="short"),
                legend=graph_legend(max=False),
                fill_gradient=0,
            ),
            graph_panel(
                title="Region",
                description="The number of Regions on each TiFlash instance",
                targets=[
                    target(
                        expr='sum(tiflash_proxy_tikv_raftstore_region_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="region", instance=~"$proxy_instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_proxy_tikv_raftstore_hibernated_peer_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}) by (instance, state)',
                        legend_format="{{instance}}-{{state}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short"),
                fill_gradient=0,
                null_point_mode="null",
                decimals=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="CPU Usage",
                description="TiFlash CPU usage calculated with process CPU running seconds.",
                targets=[
                    target(
                        expr='rate(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])',
                        legend_format="{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="limit-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit",
                    right_format="short",
                    left_min="0",
                    left_decimals=1,
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "/limit/",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                    }
                ],
            ),
            graph_panel(
                title="Memory",
                description="The memory usage per TiFlash instance",
                targets=[
                    target(
                        expr='sum(tiflash_proxy_process_resident_memory_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role", job=~".*tiflash"}) by (instance)',
                        legend_format="{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_MemoryCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="limit-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_jemalloc_retained{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="retained",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_jemalloc_mapped{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="mapped",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_jemalloc_resident{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="resident",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_jemalloc_allocated{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="allocated",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_jemalloc_active{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="active",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_jemalloc_metadata_thp{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="metadata_thp",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_jemalloc_metadata{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="metadata",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_mimalloc_current_rss{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="mimalloc_rss",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_mimalloc_current_commit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="mimalloc_commit",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_mmap_alive{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$instance", instance=~"$tiflash_role"})',
                        legend_format="mmap",
                        hide=True,
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "/limit/",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="IO Throughput",
                targets=[
                    target(
                        expr='sum(irate(tiflash_proxy_threads_io_bytes_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role", job=~".*tiflash"}[$__rate_interval])) by (instance)',
                        legend_format="{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="bytes",
                    right_format="short",
                    left_min="0",
                    left_decimals=0,
                ),
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="Remote Store Summary (Disagg arch)",
                targets=[
                    target(
                        expr='sum(tiflash_storage_s3_store_summary_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, store_id, type)',
                        legend_format="store-{{store_id}}-{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
            ),
        ]
    )
    return layout.row_panel


def ThreadsCPU() -> RowPanel:
    layout = Layout(title="Threads CPU")
    layout.row(
        [
            graph_panel(
                title="SST Import Service",
                description="Involved when importing data.",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"sst_importer.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="SST Apply",
                description="Involved when importing data.",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"apply_low_.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"apply_low_.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Region Task",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"region_task.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"region_task.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
            graph_panel(
                title="Region Worker",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"region_worker.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"region_worker.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft Store",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"raftstore_.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"raftstore_.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
            graph_panel(
                title="Apply Worker",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"apply_.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"apply_.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Storage Background (Small Tasks)",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"bg_\\\\d+", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"bg_\\\\d+", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
            graph_panel(
                title="Storage Background (Large Tasks)",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"bg_block_\\\\d+", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"bg_block_\\\\d+", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Manual Compaction",
                description="Involved when manually compacting the data.",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"m_compact_pool", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"m_compact_pool", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
            graph_panel(
                title="GRPC Async Server",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"async_poller.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"async_poller.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="GRPC Async Client",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"GRPCComp.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"GRPCComp.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
            graph_panel(
                title="FAP builder",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"fap_builder.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"fap_builder.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Snapshot Sender",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"snap_sender.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"snap_sender.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
            graph_panel(
                title="Segment Scheduler",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"segment_sched.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"segment_sched.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Local Index Pool",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"LocalIndexPool*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="pool-{{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"LocalIndexPool*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
            graph_panel(
                title="Segment Reader",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"SegmentReader.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"SegmentReader.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    return layout.row_panel


def Threads() -> RowPanel:
    layout = Layout(title="Threads")
    layout.row(
        [
            graph_panel(
                title="Threads state",
                targets=[
                    target(
                        expr='sum(tiflash_proxy_threads_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}) by (instance, state)',
                        legend_format="{{instance}}-{{state}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_proxy_threads_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="{{instance}}-total",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short"),
                fill=1,
                fill_gradient=0,
                points=True,
                pointradius=2,
                null_point_mode="null",
                decimals=1,
            ),
            graph_panel(
                title="Threads IO",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_threads_io_bytes_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[30s])) by (name, io, $additional_groupby) > 1024',
                        legend_format="{{name}}-{{io}} {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="Bps", right_format="short"),
                fill=1,
                fill_gradient=0,
                points=True,
                pointradius=2,
                null_point_mode="null",
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Thread Voluntary Context Switches",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_voluntary_context_switches{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[30s])) by (instance, name) > 200',
                        legend_format="{{instance}} - {{name}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short"),
                fill=1,
                fill_gradient=0,
                points=True,
                pointradius=2,
                null_point_mode="null",
                decimals=1,
            ),
            graph_panel(
                title="Thread Nonvoluntary Context Switches",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_nonvoluntary_context_switches{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[30s])) by (instance, name) > 50',
                        legend_format="{{instance}} - {{name}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short"),
                fill=1,
                fill_gradient=0,
                points=True,
                pointradius=2,
                null_point_mode="null",
                decimals=1,
            ),
        ]
    )
    return layout.row_panel


def Coprocessor() -> RowPanel:
    layout = Layout(title="Coprocessor")
    layout.row(
        [
            graph_panel(
                title="Request QPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_coprocessor_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Executor QPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_coprocessor_executor_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Request Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_coprocessor_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_coprocessor_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_coprocessor_request_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_coprocessor_request_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Error QPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_coprocessor_request_error{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (reason)',
                        legend_format="{{reason}}",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Request Handle Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_coprocessor_request_handle_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_coprocessor_request_handle_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_coprocessor_request_handle_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_coprocessor_request_handle_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Response Bytes/Seconds",
                targets=[
                    target(
                        expr='sum(rate(tiflash_coprocessor_response_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Cop task memory usage",
                targets=[
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_coprocessor_request_memory_usage_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type))',
                        legend_format="999-{{type}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_coprocessor_request_memory_usage_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type))',
                        legend_format="99-{{type}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='histogram_quantile(0.95, sum(rate(tiflash_coprocessor_request_memory_usage_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type))',
                        legend_format="95-{{type}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_coprocessor_request_memory_usage_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type))',
                        legend_format="80-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill=1,
                fill_gradient=0,
            ),
            graph_panel(
                title="Exchange Bytes/Seconds",
                targets=[
                    target(
                        expr='sum(rate(tiflash_exchange_data_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Threads of Rpc",
                targets=[
                    target(
                        expr='max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"rpc.*", type!~".*max"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Handling Request Number",
                targets=[
                    target(
                        expr='sum(tiflash_coprocessor_handling_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type)',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="none", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Threads",
                targets=[
                    target(
                        expr='max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~".*max", type!~"rpc.*"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Max Threads of Rpc",
                targets=[
                    target(
                        expr='max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"rpc.*", type=~".*max"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="MPP Query count",
                description="The MPP query count in TiFlash",
                targets=[
                    target(
                        expr='max(tiflash_mpp_task_manager{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Max Threads",
                targets=[
                    target(
                        expr='max(tiflash_thread_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*max", type!~"rpc.*"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Time of the Longest Live MPP Task",
                targets=[
                    target(
                        expr='tiflash_mpp_task_monitor{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Data size in send and receive queue",
                targets=[
                    target(
                        expr='tiflash_exchange_queueing_data_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Network Transmission",
                targets=[
                    target(
                        expr='sum(rate(tiflash_network_transmission_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Establish calldata details",
                description="The establish calldata details",
                targets=[
                    target(
                        expr='max(tiflash_establish_calldata_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type != "new_request_calldata"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    return layout.row_panel


def TaskScheduler() -> RowPanel:
    layout = Layout(title="Task Scheduler")
    layout.row(
        [
            graph_panel(
                title="Min TSO",
                description="the min_tso of each instance",
                targets=[
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="min_tso"}) by (instance, resource_group)',
                        legend_format="{{instance}}-{{resource_group}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="none",
                    right_format="short",
                    left_label="TSO",
                    left_show=False,
                ),
                legend=graph_legend(max=False),
                fill=1,
                fill_gradient=0,
                points=True,
                pointradius=1,
                null_point_mode="null",
            ),
            graph_panel(
                title="Estimated Thread Usage and Limit",
                description="estimated thread usage in min-tso scheduler, and the sort/hard limit of estimated thread in scheduler.",
                targets=[
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="thread_soft_limit"}) by (instance, type, resource_group)',
                        legend_format="{{instance}}-{{type}}-{{resource_group}}",
                    ),
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="estimated_thread_usage"}) by (instance, type, resource_group)',
                        legend_format="{{instance}}-{{type}}-{{resource_group}}",
                    ),
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="thread_hard_limit"}) by (instance, type, resource_group)',
                        legend_format="{{instance}}-{{type}}-{{resource_group}}",
                    ),
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="global_estimated_thread_usage"}) by (instance, type, resource_group)',
                        legend_format="{{instance}}-{{type}}-{{resource_group}}",
                    ),
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="group_entry_count"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="none",
                    right_format="short",
                    left_label="Threads",
                    left_log_base=10,
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Active and Waiting Queries Count",
                description="the count of active/ waiting queries",
                targets=[
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="waiting_queries_count"}) by (instance, type, resource_group)',
                        legend_format="{{instance}}-{{type}}-{{resource_group}}",
                    ),
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="active_queries_count"}) by (instance, type, resource_group)',
                        legend_format="{{instance}}-{{type}}-{{resource_group}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="none", right_format="short", left_label="Queries"
                ),
                fill_gradient=0,
            ),
            graph_panel(
                title="Active and Waiting Tasks Count",
                description="the count of active/ waiting tasks",
                targets=[
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="waiting_tasks_count"}) by (instance, type, resource_group)',
                        legend_format="{{instance}}-{{type}}-{{resource_group}}",
                    ),
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="active_tasks_count"}) by (instance, type, resource_group)',
                        legend_format="{{instance}}-{{type}}-{{resource_group}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="none", right_format="short", left_label="Tasks"
                ),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Hard Limit Exceeded Count",
                description="the usage of estimated threads exceeded the hard limit where errors occur.",
                targets=[
                    target(
                        expr='max(tiflash_task_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="hard_limit_exceeded_count"}) by (instance, type, resource_group)',
                        legend_format="{{instance}}-{{resource_group}}",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Task Waiting Duration",
                description="the time of waiting for schedule",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, instance, resource_group, $additional_groupby) / 1000000000)',
                        legend_format="{{instance}}-{{resource_group}}-max",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, instance, resource_group, $additional_groupby))',
                        legend_format="{{instance}}-{{resource_group}}-9999",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, instance, resource_group, $additional_groupby))',
                        legend_format="{{instance}}-{{resource_group}}-999",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, instance, resource_group, $additional_groupby))',
                        legend_format="{{instance}}-{{resource_group}}-99",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_task_scheduler_waiting_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, instance, resource_group, $additional_groupby))',
                        legend_format="{{instance}}-{{resource_group}}-80",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_task_scheduler_waiting_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, resource_group, $additional_groupby) / sum(rate(tiflash_task_scheduler_waiting_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, resource_group, $additional_groupby))',
                        legend_format="{{instance}}-{{resource_group}}-avg",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    return layout.row_panel


def DDL() -> RowPanel:
    layout = Layout(title="DDL")
    layout.row(
        [
            graph_panel(
                title="Schema Internal DDL OPM",
                description="Executed DDL jobs per minute",
                targets=[
                    target(
                        expr='avg(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="total",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type,instance)',
                        legend_format="{{type}}-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(increase(tiflash_schema_internal_ddl_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="total-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="opm", right_format="none", left_min="0"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill_gradient=0,
            ),
            graph_panel(
                title="Schema Apply OPM",
                description="Executed DDL apply jobs per minute",
                targets=[
                    target(
                        expr='avg(increase(tiflash_schema_trigger_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="triggle-by-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="opm", right_format="none", left_min="0"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill_gradient=0,
            ),
        ]
    )
    layout.half_row(
        [
            graph_panel(
                title="Schema Apply Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_schema_apply_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_schema_apply_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_schema_apply_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_schema_apply_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_schema_apply_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_schema_apply_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_schema_apply_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(tiflash_sync_schema_applying{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"$type"}) by (instance)',
                        legend_format="applying-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="s", right_format="short", left_min="0", right_show=True
                ),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
                series_overrides=[{"alias": "/^applying/", "yaxis": 2}],
            ),
        ]
    )
    return layout.row_panel


def ImbalanceReadWrite() -> RowPanel:
    layout = Layout(title="Imbalance read/write")
    layout.row(
        [
            graph_panel(
                title="CPU Usage (irate)",
                description="TiFlash CPU usage calculated with process CPU running seconds.",
                targets=[
                    target(
                        expr='irate(tiflash_proxy_process_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$tiflash_role"}[$__rate_interval])',
                        legend_format="{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_LogicalCPUCores{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="limit-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit",
                    right_format="short",
                    left_min="0",
                    left_decimals=1,
                ),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "/limit/",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                    }
                ],
            ),
            graph_panel(
                title="Segment Reader",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"SegmentReader.*", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"SegmentReader.*", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Request QPS by instance",
                targets=[
                    target(
                        expr='sum(rate(tiflash_coprocessor_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, instance)',
                        legend_format="{{type}}-{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Read Throughput by instance",
                description="The flow of different kinds of read operations",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="File Descriptor-{{instance}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="Page-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="PageBackGround-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="binBps", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Write Command OPS By Instance",
                description="The total count of different kinds of commands received",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_DMWriteBlock{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, type)',
                        legend_format="write block-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, type)',
                        legend_format="{{type}}-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="opm",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                fill_gradient=0,
                series_overrides=[{"alias": "/delete_range|ingest/", "yaxis": 2}],
            ),
            graph_panel(
                title="Write Throughput By Instance",
                description="The throughput of write by instance",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write"}[$__rate_interval])) by (instance)',
                        legend_format="write-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ingest"}[$__rate_interval])) by (instance)',
                        legend_format="ingest-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="binBps",
                    right_format="bytes",
                    left_min="0",
                    right_show=True,
                ),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
                series_overrides=[{"alias": "/total/", "yaxis": 2}],
            ),
        ]
    )
    return layout.row_panel


def MemoryTrace() -> RowPanel:
    layout = Layout(title="Memory trace")
    layout.row(
        [
            graph_panel(
                title="Number of Keyspaces",
                targets=[
                    target(
                        expr='tiflash_system_current_metric_NumKeyspace{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="keyspace-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="s"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="Number of Physical Tables",
                targets=[
                    target(
                        expr='tiflash_system_current_metric_DT_NumStorageDeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="tables-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_NumIStorage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="tables-all-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="s"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Number of Segments",
                targets=[
                    target(
                        expr='tiflash_system_current_metric_DT_NumSegment{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="segments-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_DT_NumMemTable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="mem_table-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="s"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="Bytes of MemTables",
                targets=[
                    target(
                        expr='tiflash_system_current_metric_DT_BytesMemTable{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="bytes-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_DT_BytesMemTableAllocated{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="bytes-allocated-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="s"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Mark Cache and Minmax Index Cache Memory Usage",
                description="The memory usage of mark cache and minmax index cache",
                targets=[
                    target(
                        expr='tiflash_system_asynchronous_metric_MarkCacheBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="mark_cache_{{instance}}",
                    ),
                    target(
                        expr='tiflash_system_asynchronous_metric_MinMaxIndexFiles{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="minmax_index_cache_{{instance}}",
                    ),
                    target(
                        expr='tiflash_system_asynchronous_metric_RNMVCCIndexCacheBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="rn_mvcc_index_cache_{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "/limit/",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                    }
                ],
            ),
            graph_panel(
                title="Effectiveness of Mark Cache",
                description="cache misses or cache hits of mark_cache.\nBased on this infactor, we can check whether mark_cache is large enough",
                targets=[
                    target(
                        expr='max(tiflash_system_profile_event_MarkCacheMisses{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="mark cache misses",
                    ),
                    target(
                        expr='max(tiflash_system_profile_event_MarkCacheHits{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="mark cache hits",
                    ),
                ],
                yaxes=yaxes(left_format="percentunit", right_format="percent"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Schema of Column File",
                description="Information about schema of column file, to learn the memory usage of schema",
                targets=[
                    target(
                        expr='max(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"current_size"}) by (instance)',
                        legend_format="current_size-{{instance}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"hit_count"}[$__rate_interval])) by (instance)',
                        legend_format="hit_count_ops-{{instance}}",
                    ),
                    target(
                        expr='max(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"still_used_when_evict"}) by (instance)',
                        legend_format="still_used_when_evict-{{instance}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_shared_block_schemas{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"miss_count"}[$__rate_interval])) by (instance)',
                        legend_format="miss_count_ops-{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short"),
                legend=graph_legend(max=False),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="Read Snapshots",
                targets=[
                    target(
                        expr='tiflash_system_current_metric_DT_SegmentReadTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="read_tasks-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_asynchronous_metric_MaxDTDeltaOldestSnapshotLifetime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="max_snapshot_lifetime-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="short",
                    right_format="s",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[{"alias": "/max_snapshot_lifetime/", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Memory by thread",
                targets=[
                    target(
                        expr='rate(tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}[$__interval])',
                        legend_format="{{instance}}-{{type}}",
                    ),
                    target(
                        expr='-rate(tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}[$__interval])',
                        legend_format="{{instance}}-{{type}}",
                    ),
                    target(
                        expr='tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}',
                        legend_format="{{instance}}-{{type}}-tot",
                        hide=True,
                    ),
                    target(
                        expr='-tiflash_storages_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}',
                        legend_format="{{instance}}-{{type}}-tot",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="Memory by thread (proxy)",
                targets=[
                    target(
                        expr='rate(tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}[$__interval])',
                        legend_format="{{instance}}-{{type}}",
                    ),
                    target(
                        expr='-rate(tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}[$__interval])',
                        legend_format="{{instance}}-{{type}}",
                    ),
                    target(
                        expr='tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"alloc_.*"}',
                        legend_format="{{instance}}-{{type}}-tot",
                        hide=True,
                    ),
                    target(
                        expr='-tiflash_raft_proxy_thread_memory_usage{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dealloc_.*"}',
                        legend_format="{{instance}}-{{type}}-tot",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Memory by class",
                targets=[
                    target(
                        expr='tiflash_memory_usage_by_class{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="{{instance}}-{{type}}",
                    ),
                    target(
                        expr='rate(tiflash_memory_usage_by_class{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__interval])',
                        legend_format="{{instance}}-{{type}}-rate",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="KVStore memory",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_MemoryTrackingKVStore{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
        ]
    )
    return layout.row_panel


def ColumnarStorage() -> RowPanel:
    layout = Layout(title="Columnar Storage")
    layout.row(
        [
            graph_panel(
                title="IA usage",
                targets=[
                    target(
                        expr='tiflash_proxy_kv_engine_ia_main_queue_capacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
                        legend_format="capacity-main-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_proxy_kv_engine_ia_small_queue_capacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
                        legend_format="capacity-small-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_proxy_kv_engine_ia_manager_segments_memory_capacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
                        legend_format="capacity-segments-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_proxy_kv_engine_ia_manager_segments_memory_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
                        legend_format="segments-mem-size-{{instance}}",
                    ),
                    target(
                        expr='tiflash_proxy_kv_engine_ia_manager_segments_disk_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
                        legend_format="segments-disk-size-{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "/limit/",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                    }
                ],
            ),
            graph_panel(
                title="IA Segments Memory Wait",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, $additional_groupby) / 1000000000)',
                        legend_format="max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby) / sum(rate(tiflash_proxy_kv_engine_ia_manager_segments_memory_wait_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby))',
                        legend_format="avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="IA Segment Remote Read Cache",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_cache_hit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="cache-hit {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_cache_miss{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="cache-miss {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="IA Segments Remote Read Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, $additional_groupby) / 1000000000)',
                        legend_format="max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby) / sum(rate(tiflash_proxy_kv_engine_ia_remote_read_segment_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby))',
                        legend_format="avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="ColumnarFile Cache",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_kv_engine_columnar_file_cache_hit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="file-cache-hit {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_proxy_kv_engine_columnar_file_cache_miss{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="file-cache-miss {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Columnar Prefetch Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, $additional_groupby) / 1000000000)',
                        legend_format="max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby) / sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby))',
                        legend_format="avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Columnar Prefetch Cache Hit Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, $additional_groupby) / 1000000000)',
                        legend_format="max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby) / sum(rate(tiflash_proxy_kv_engine_columnar_prefetch_cache_hit_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby))',
                        legend_format="avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Columnar Fetch Snapshot Retry",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_retry_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="retry {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Columnar Fetch Snapshot Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, $additional_groupby) / 1000000000)',
                        legend_format="max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby) / sum(rate(tiflash_proxy_kv_engine_columnar_fetch_snapshot_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby))',
                        legend_format="avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Columnar Meta Cache",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_hit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="hit {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_miss{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="miss {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_proxy_kv_engine_columnar_meta_cache_parse{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="parse {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Columnar Meta Cache Gauge",
                targets=[
                    target(
                        expr='tiflash_proxy_kv_engine_columnar_meta_cache_entries{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
                        legend_format="entries-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_proxy_kv_engine_columnar_meta_cache_weighted_size{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
                        legend_format="weighted_size-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="bytes",
                    right_format="short",
                    left_min="0",
                    right_show=True,
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[{"alias": "/entries/", "yaxis": 2}],
            ),
        ]
    )
    return layout.row_panel


def Storage() -> RowPanel:
    layout = Layout(title="Storage")
    layout.row(
        [
            graph_panel(
                title="Write Command OPS",
                description="The total count of different kinds of commands received",
                targets=[
                    target(
                        expr='sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_DMWriteBlock{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="write block",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="opm",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill_gradient=0,
                series_overrides=[{"alias": "/delete_range|ingest/", "yaxis": 2}],
            ),
            graph_panel(
                title="Write Amplification",
                targets=[
                    target(
                        expr='sum by (instance) ( tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} + tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"} ) / sum by (instance) ( tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"} )',
                        legend_format="amp-total-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[5m]) )',
                        legend_format="amp-5min-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[10m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[10m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[10m]) )',
                        legend_format="amp-10min-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[30m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[30m]) ) / sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[30m]) )',
                        legend_format="amp-30min-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum by (instance) ( rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) + rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) )',
                        legend_format="fs-5min-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum by (instance) ( rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[5m]) )',
                        legend_format="write-5min-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="short",
                    right_format="binBps",
                    left_min="0",
                    left_max="20",
                    right_show=True,
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[{"alias": "/fs|write/", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="SubTasks Write Throughput (bytes)",
                description="The throughput of (maybe foreground) tasks of storage in bytes",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_subtask_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="binBps",
                    right_format="bytes",
                    left_min="0",
                    right_show=True,
                ),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
                series_overrides=[{"alias": "/total/", "yaxis": 2}],
            ),
            graph_panel(
                title="SubTasks Write Throughput (rows)",
                description="The throughput of (maybe foreground) tasks of storage in rows",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_subtask_throughput_rows{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="none",
                    right_format="bytes",
                    left_min="0",
                    right_show=True,
                ),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
                series_overrides=[{"alias": "/total/", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Small Internal Tasks OPS",
                description="Total number of storage's internal sub tasks",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_subtask_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Small Internal Tasks Duration",
                description="Duration of storage's internal sub tasks",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_subtask_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_subtask_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ],
        height=5,
    )
    layout.row(
        [
            graph_panel(
                title="Large Internal Tasks OPS",
                description="Total number of storage's internal sub tasks",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_subtask_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Large Internal Tasks Duration",
                description="Duration of storage's internal sub tasks",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_subtask_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_subtask_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"(delta_merge|seg_merge|seg_split).*"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ],
        height=5,
    )
    layout.row(
        [
            graph_panel(
                title="Current Data Management Tasks",
                description="The current processing number of  segments' background management",
                targets=[
                    target(
                        expr='avg(tiflash_system_current_metric_DT_DeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="delta_merge-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='avg(tiflash_system_current_metric_DT_SegmentSplit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="seg_split-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='avg(tiflash_system_current_metric_DT_SegmentMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="seg_merge-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="short",
                    right_format="none",
                    left_min="0",
                    left_decimals=0,
                ),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Opened File Count",
                description="The number of currently opened file descriptors.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)",
                targets=[
                    target(
                        expr='tiflash_proxy_process_open_fds{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", job=~".*tiflash", instance=~"$proxy_instance", instance=~"$tiflash_role"}',
                        legend_format="{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_OpenFileForWrite{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="W-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_OpenFileForRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="R-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_OpenFileForReadWrite{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="RW-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="File Open OPS",
                description="The number of open file descriptors action.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_FileOpen{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="Open-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_FileOpenFailed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="OpenFail-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="short", left_min="0"),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="FSync Status",
                description="OPS and duration of fsync operations.\n(Only counting storage engine of TiFlash by now. Not including TiFlash-Proxy)",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_FileFSync{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="ops-fsync-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_system_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"fsync"}[$__rate_interval]))) by (le, instance) / 1000000000)',
                        legend_format="max-fsync-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops", right_format="s", left_min="0", right_show=True
                ),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                series_overrides=[{"alias": "/max-fsync/", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Disk Write OPS",
                description="The number of different kinds of read operations",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMWriteIOCalls{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="Page",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMWritePages{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="PageFile",
                        hide=True,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWrite{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="File Descriptor",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Disk Read OPS",
                description="The number of different kinds of read operations",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMReadIOCalls{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="Page",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMReadPages{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="PageFile",
                        hide=True,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="File Descriptor",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Write flow",
                description="The flow of different kinds of write operations",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_WriteBufferFromFileDescriptorWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="File Descriptor",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="Page",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMBackgroundWriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="PageBackGround",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="binBps", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
            graph_panel(
                title="Read flow",
                description="The flow of different kinds of read operations",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_ReadBufferFromFileDescriptorReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="File Descriptor",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="Page",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_PSMBackgroundReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="PageBackGround",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="binBps", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Compression Ratio",
                description="The compression ratio of different compression algorithm",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lz4_uncompressed_bytes"}[$__rate_interval]))/sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lz4_compressed_bytes"}[$__rate_interval]))',
                        legend_format="lz4",
                    ),
                    target(
                        expr='sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lightweight_uncompressed_bytes"}[$__rate_interval]))/sum(rate(tiflash_storage_pack_compression_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"lightweight_compressed_bytes"}[$__rate_interval]))',
                        legend_format="lightweight",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short"),
                legend=graph_legend(avg=True, max=False),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="Compression Algorithm Count",
                description="The count of the compression algorithm used by each data part",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_pack_compression_algorithm_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short"),
                legend=graph_legend(max=False, total=True),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
        ]
    )
    return layout.row_panel


def StorageReadPoolDataSharing() -> RowPanel:
    layout = Layout(title="Storage Read Pool & Data Sharing")
    layout.row(
        [
            graph_panel(
                title="Read Tasks OPS",
                description="Total number of storage engine read tasks",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_read_tasks_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Read Snapshots",
                targets=[
                    target(
                        expr='tiflash_system_current_metric_DT_SegmentReadTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="read_tasks-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_PSMVCCSnapshotsList{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="snapshot_list-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_PSMVCCNumSnapshots{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="num_snapshot-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_DT_SnapshotOfRead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="read-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_DT_SnapshotOfReadRaw{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="read_raw-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_DT_SnapshotOfDeltaMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="delta_merge-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_DT_SnapshotOfDeltaCompact{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="delta_compact-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_DT_SnapshotOfSegmentMerge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="seg_merge-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_DT_SnapshotOfSegmentSplit{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="seg_split-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_current_metric_DT_SnapshotOfPlaceIndex{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="place_index-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_system_asynchronous_metric_MaxDTDeltaOldestSnapshotLifetime{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="max_snapshot_lifetime-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="short",
                    right_format="s",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[{"alias": "/max_snapshot_lifetime/", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Read Thread Internal Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_read_thread_internal_us_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_read_thread_internal_us_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_read_thread_internal_us_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_read_thread_internal_us_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_read_thread_internal_us_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_read_thread_internal_us_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_read_thread_internal_us_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="µs", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Read Thread Scheduling",
                description="The information of read thread scheduling.",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_read_thread_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ru_exhausted|sche_active_segment_limit|sche_from_cache|sche_new_task|sche_no_pool|sche_no_ru|sche_no_segment|sche_no_slot|push_block_bytes"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="binBps",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                legend=graph_legend(current=False, max=False),
                fill_gradient=0,
                series_overrides=[{"alias": "/push_block/", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Data Sharing",
                description="The information of data sharing cache hit ratio. Data sharing cache is purpose-built for OLAP workload that can reduce repeated data reads of concurrent table scanning.",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_read_thread_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"add_cache_total_bytes_limit"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_column_cache_packs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"data_sharing_hit"}[$__rate_interval])) / sum(rate(tiflash_storage_column_cache_packs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"data_sharing_hit|data_sharing_miss"}[$__rate_interval])))',
                        legend_format="data_sharing_cache_hit_ratio",
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_column_cache_packs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"extra_column_hit"}[$__rate_interval])) / sum(rate(tiflash_storage_column_cache_packs{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"extra_column_hit|extra_column_miss"}[$__rate_interval])))',
                        legend_format="extra_column_cache_hit_ratio",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="percentunit",
                    left_min="0",
                    right_show=True,
                ),
                fill_gradient=0,
                series_overrides=[
                    {"alias": "/cache_hit_ratio/", "yaxis": 2},
                    {"alias": "/cache_hit_ratio/", "yaxis": 2},
                ],
            ),
            graph_panel(
                title="Segment MergedTask",
                targets=[
                    target(
                        expr='sum(tiflash_storage_read_thread_gauge{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="percentunit",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                fill_gradient=0,
                series_overrides=[{"alias": "/cache_hit_ratio/", "yaxis": 2}],
            ),
            graph_panel(
                title="Segment MergedTask Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_read_thread_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_read_thread_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_read_thread_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_read_thread_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_read_thread_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_read_thread_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_read_thread_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="VersionChain",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_version_chain_ms_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_version_chain_ms_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_version_chain_ms_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_version_chain_ms_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_version_chain_ms_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_version_chain_ms_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_version_chain_ms_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="ms", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="DeltaIndexError",
                description="Errors of DeltaIndex",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_DTDeltaIndexError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="DeltaIndexError-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="cps", right_format="opm", left_min="0"),
                legend=graph_legend(current=False),
                fill_gradient=0,
            ),
        ]
    )
    return layout.row_panel


def PageStorage() -> RowPanel:
    layout = Layout(title="PageStorage")
    layout.row(
        [
            graph_panel(
                title="PageStorage Disk Usage",
                description="The disk usage of PageStorage instances in each TiFlash node",
                targets=[
                    target(
                        expr='tiflash_system_asynchronous_metric_BlobDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="blob_disk_size-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_BlobValidBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="blob_valid_size-{{instance}}",
                    ),
                    target(
                        expr='sum((tiflash_system_asynchronous_metric_BlobValidBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) / (tiflash_system_asynchronous_metric_BlobDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"})) by (instance)',
                        legend_format="blob_valid_rate-{{instance}}",
                    ),
                    target(
                        expr='tiflash_system_asynchronous_metric_LogDiskBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="log_size-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="bytes",
                    right_format="percentunit",
                    left_min="0",
                    right_min="0",
                    right_max="1.1",
                    right_show=True,
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
                series_overrides=[
                    {"alias": "/^valid_rate/", "yaxis": 2},
                    {"alias": "/size/", "linewidth": 3},
                ],
            ),
            graph_panel(
                title="PageStorage File Num",
                description="The number of files of PageStorage instances in each TiFlash node",
                targets=[
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_BlobFileNums{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="blob_file-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_asynchronous_metric_LogNums{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="log_file-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="short", right_format="percentunit", left_min="0"
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="PageStorage WriteBatch Size",
                y_format="bytes",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_storage_page_write_batch_size_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="v3"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
            graph_panel(
                title="Page write Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="{{type}}-max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_page_write_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_page_write_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_page_write_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="{{type}}-avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Page GC Tasks OPM",
                targets=[
                    target(
                        expr='sum(increase(tiflash_storage_page_gc_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="opm", right_format="short", left_min="0"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill=1,
                fill_gradient=0,
            ),
            graph_panel(
                title="Page GC Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_page_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="{{type}}-max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_page_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_page_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_page_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_page_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_page_gc_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_page_gc_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="{{type}}-avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Numer of Pages",
                description="The number of pages of all TiFlash instance",
                targets=[
                    target(
                        expr='tiflash_system_asynchronous_metric_PagesInMem{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="num_pages-{{instance}}",
                    ),
                    target(
                        expr='tiflash_system_asynchronous_metric_VersionedEntries{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}',
                        legend_format="num_entries-{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short", left_min="0"),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
            ),
            graph_panel(
                title="PageStorage Pending Writers Num",
                description="The num of pending writers in PageStorage",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_PSPendingWriterNum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="size-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="PageStorage stored bytes by type",
                targets=[
                    target(
                        expr='sum(tiflash_storage_page_data_by_types{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
            graph_panel(
                title="Number of Tables",
                description="The number of tables running under different mode in DeltaTree",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_StoragePoolV2Only{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="V2-{{instance}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_StoragePoolV3Only{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="V3-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_StoragePoolMixMode{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Mix-{{instance}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_StoragePoolUniPS{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="UniPS-{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short"),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="PS Command OPS By Instance",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_page_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, type)',
                        legend_format="{{type}}-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="opm",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                fill_gradient=0,
                series_overrides=[{"yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="PS Apply edits OPS By Instance",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_page_apply_edit_type{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, type)',
                        legend_format="{{type}}-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="opm",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                fill_gradient=0,
                series_overrides=[{"yaxis": 2}],
            ),
        ]
    )
    return layout.row_panel


def RateLimiter() -> RowPanel:
    layout = Layout(title="Rate Limiter")
    layout.row(
        [
            graph_panel(
                title="I/O Limiter Throughput",
                description="The storage I/O limiter metrics.",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_io_limiter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, instance)',
                        legend_format="{{type}}-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="binBps",
                    right_format="short",
                    left_min="0",
                    left_decimals=0,
                ),
                fill=1,
                fill_gradient=0,
            ),
            graph_panel(
                title="I/O Limiter Threshold",
                description="Current limit bytes per second of Storage I/O limiter",
                targets=[
                    target(
                        expr='sum(tiflash_storage_io_limiter_curr{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type, instance)',
                        legend_format="{{type}}-{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short"),
                legend=graph_legend(max=False),
                fill=1,
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="I/O Limiter Current Pending Gauge",
                description="I/O Limiter current pending gauge.",
                targets=[
                    target(
                        expr='avg(tiflash_system_current_metric_RateLimiterPendingWriteRequest{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="other-current-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='avg(tiflash_system_current_metric_IOLimiterPendingBgWriteReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="bgwrite-current-{{instance}}",
                    ),
                    target(
                        expr='avg(tiflash_system_current_metric_IOLimiterPendingFgWriteReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="fgwrite-current-{{instance}}",
                    ),
                    target(
                        expr='avg(tiflash_system_current_metric_IOLimiterPendingBgReadReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="bgread-current-{{instance}}",
                    ),
                    target(
                        expr='avg(tiflash_system_current_metric_IOLimiterPendingFgReadReq{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="fgread-current-{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="s", right_show=True),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[{"alias": "/pending/", "yaxis": 2}],
            ),
            graph_panel(
                title="I/O Limiter Pending OPS",
                description="The storage I/O limiter metrics.",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_io_limiter_pending_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, instance)',
                        legend_format="{{type}}-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="s",
                    left_min="0",
                    left_decimals=0,
                    right_show=True,
                ),
                fill=1,
                fill_gradient=0,
                series_overrides=[{"alias": "", "yaxis": 2}],
            ),
            graph_panel(
                title="I/O Limiter Pending Duration",
                description="I/O Limiter pending duration.",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_io_limiter_pending_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="{{type}}-pending-max",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-pending-9999",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-pending-999",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-pending-99",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_io_limiter_pending_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-pending-80",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_io_limiter_pending_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_io_limiter_pending_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="{{type}}-pending-avg",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    return layout.row_panel


def StorageWriteStall() -> RowPanel:
    layout = Layout(title="Storage Write Stall")
    layout.row(
        [
            graph_panel(
                title="Write Stall Duration",
                description="The stall duration of write and delete range",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_write_stall_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, instance, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}}-{{instance}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_write_stall_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, instance, $additional_groupby))',
                        legend_format="9999-{{type}}-{{instance}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_write_stall_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, instance, $additional_groupby))',
                        legend_format="999-{{type}}-{{instance}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_write_stall_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, instance, $additional_groupby))',
                        legend_format="99-{{type}}-{{instance}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_write_stall_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, instance, $additional_groupby))',
                        legend_format="80-{{type}}-{{instance}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_write_stall_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, instance, $additional_groupby) / sum(rate(tiflash_storage_write_stall_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, instance, $additional_groupby))',
                        legend_format="avg-{{type}}-{{instance}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(
                    left_format="s", right_format="short", left_min="0", right_show=True
                ),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
                series_overrides=[{"alias": "99-delta_merge", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Write & Delta Management Throughput",
                description="The throughput of write and delta's background management",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"}[$__rate_interval]))',
                        legend_format="write+ingest",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"write|ingest"}[$__rate_interval]))',
                        legend_format="ManageDelta",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="binBps", right_format="bytes", left_min="0"),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
            graph_panel(
                title="Write & Delta Management Total",
                description="The throughput of write and delta's background management",
                targets=[
                    target(
                        expr='sum(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write|ingest"})',
                        legend_format="write+ingest",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!~"write|ingest"})',
                        legend_format="ManageDelta",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="bytes", left_min="0"),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Write Throughput By Instance",
                description="The throughput of write by instance",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write"}[$__rate_interval])) by (instance)',
                        legend_format="write-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_storage_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ingest"}[$__rate_interval])) by (instance)',
                        legend_format="ingest-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="binBps",
                    right_format="bytes",
                    left_min="0",
                    right_show=True,
                ),
                legend=graph_legend(side_width=250),
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
                series_overrides=[{"alias": "/total/", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Write Command OPS By Instance",
                description="The total count of different kinds of commands received",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_DMWriteBlock{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, type)',
                        legend_format="write block-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(increase(tiflash_storage_command_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, type)',
                        legend_format="{{type}}-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="opm",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                fill_gradient=0,
                series_overrides=[{"alias": "/delete_range|ingest/", "yaxis": 2}],
            ),
        ]
    )
    return layout.row_panel


def Raft() -> RowPanel:
    layout = Layout(title="Raft")
    layout.row(
        [
            graph_panel(
                title="Stale Read OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_stale_read_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Raft Read Index OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_raft_read_index_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Learner Read Failures",
                targets=[
                    target(
                        expr='sum(rate(tiflash_raft_learner_read_failures_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Read Index Events",
                targets=[
                    target(
                        expr='sum(rate(tiflash_raft_read_index_events_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft Wait Index Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_raft_wait_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, $additional_groupby) / 1000000000)',
                        legend_format="max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_raft_wait_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_wait_index_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby) / sum(rate(tiflash_raft_wait_index_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby))',
                        legend_format="avg {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(increase(tiflash_system_profile_event_RaftWaitIndexTimeout{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{instance}}-timeout",
                    ),
                ],
                yaxes=yaxes(
                    left_format="s", right_format="opm", left_min="0", right_show=True
                ),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
                series_overrides=[{"alias": "/timeout/", "yaxis": 2}],
            ),
            graph_panel(
                title="Raft Batch Read Index Duration",
                description="The number of currently applying snapshots.",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_raft_read_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, $additional_groupby) / 1000000000)',
                        legend_format="max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_raft_read_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_raft_read_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_raft_read_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_raft_read_index_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_read_index_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby) / sum(rate(tiflash_raft_read_index_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby))',
                        legend_format="avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Apply Raft write logs Duration",
                description="Duration of applying Raft write logs",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_apply_write_command_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_raft_apply_write_command_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_apply_write_command_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="write"}[$__rate_interval])) / sum(rate(tiflash_raft_apply_write_command_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="write"}[$__rate_interval])))',
                        legend_format="avg-write",
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_apply_write_command_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="admin"}[$__rate_interval])) / sum(rate(tiflash_raft_apply_write_command_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="admin"}[$__rate_interval])))',
                        legend_format="avg-admin",
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_apply_write_command_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="flush_region"}[$__rate_interval])) / sum(rate(tiflash_raft_apply_write_command_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="flush_region"}[$__rate_interval])))',
                        legend_format="avg-flush_region",
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_write_data_to_storage_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="decode"}[$__rate_interval])) / sum(rate(tiflash_raft_write_data_to_storage_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="decode"}[$__rate_interval])))',
                        legend_format="avg-decode",
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Region write Duration (decode)",
                description='Duration of decoding Region data into blocks when writing Region data to the storage layer. (Mixed with "write logs" and "apply Snapshot" operations)',
                y_format="s",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_write_data_to_storage_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="decode"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
            make_heatmap(
                title="Region write Duration (write blocks)",
                description='Duration of writing Region data blocks to the storage layer (Mixed with "write logs" and "apply Snapshot" operations)',
                y_format="s",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_write_data_to_storage_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="write"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Apply Raft write logs Duration [Heatmap]",
                description="Duration of applying Raft write logs",
                y_format="s",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="write"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
            make_heatmap(
                title="Apply Raft admin logs Duration [Heatmap]",
                description="Duration of applying Raft write logs",
                y_format="s",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_apply_write_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="admin"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft Events QPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_raft_raft_events_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Raft Frequent Events QPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_raft_raft_frequent_events_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Raft Log Gap Heatmap",
                y_format="none",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_raft_log_gap_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"applied_index"}[$__rate_interval])) by (le, type)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                    target(
                        expr='sum(delta(tiflash_raft_raft_log_gap_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"compact_index"}[$__rate_interval])) by (le, type)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
            make_heatmap(
                title="Raft Entry Batch Size Heatmap",
                y_format="none",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_entry_size_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"normal"}[$__rate_interval])) by (le, type)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Region Size (by event) Heatmap",
                y_format="bytes",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_region_flush_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"unflushed"}[$__rate_interval])) by (le, type)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                    target(
                        expr='sum(delta(tiflash_raft_region_flush_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"flushed"}[$__rate_interval])) by (le, type)',
                        legend_format="{{le}}",
                        interval_factor=2,
                        hide=True,
                    ),
                ],
            ),
            make_heatmap(
                title="Big Write To Region Size Heatmap",
                y_format="bytes",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_write_flow_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"big_write_to_region"}[$__rate_interval])) by (le, type)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Write Committed Size Heatmap",
                y_format="bytes",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_write_flow_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"write_committed"}[$__rate_interval])) by (le, type)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft Eager GC OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_raft_eager_gc_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Raft Eager GC Duration",
                description="Duration of Raft logs eager GC tasks",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_raft_eager_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_raft_eager_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_raft_eager_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_raft_eager_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_raft_eager_gc_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_eager_gc_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_raft_eager_gc_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Keys flow",
                description="The keys flow of different kinds of Raft operations",
                targets=[
                    target(
                        expr='sum(rate(tiflash_raft_process_keys{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Raft throughput",
                targets=[
                    target(
                        expr='sum(rate(tiflash_raft_throughput_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Upstream Latency [Heatmap]",
                description="Latency that TiKV sends raft log to TiFlash.",
                y_format="s",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
            graph_panel(
                title="Upstream Latency",
                description="Latency that TiKV sends raft log to TiFlash.",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, $additional_groupby) / 1000000000)',
                        legend_format="max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_raft_upstream_latency_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, $additional_groupby))',
                        legend_format="80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_upstream_latency_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby) / sum(rate(tiflash_raft_upstream_latency_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by ($additional_groupby))',
                        legend_format="avg {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.half_row(
        [
            graph_panel(
                title="Log Replication Rejected",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_tikv_server_raft_append_rejects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill_gradient=0,
            ),
        ]
    )
    return layout.row_panel


def RaftSnapshotIngestSST() -> RowPanel:
    layout = Layout(title="Raft Snapshot / IngestSST")
    layout.row(
        [
            graph_panel(
                title="Heavy Raft Apply Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_raft_command_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_raft_command_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Applying snapshots Count",
                description="The number of currently applying snapshots.",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_RaftNumSnapshotsPendingApply{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Pending-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_RaftNumPrehandlingSubTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="PrehandleSubtasks-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_RaftNumParallelPrehandlingTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="ParallelTasks-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_RaftNumWaitedParallelPrehandlingTasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Pending-ParallelTasks-{{instance}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Snapshot Uncommitted Size Heatmap",
                y_format="bytes",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_write_flow_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"snapshot_uncommitted"}[$__rate_interval])) by (le, type)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
            graph_panel(
                title="Ongoing raft snapshot",
                targets=[
                    target(
                        expr='sum(rate(tiflash_raft_ongoing_snapshot_total_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{le}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short"),
                legend=graph_legend(
                    current=False, max=False, align_as_table=False, right_side=False
                ),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Snapshot Size Heatmap",
                y_format="bytes",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_snapshot_total_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="approx_raft_snapshot"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
            make_heatmap(
                title="Snapshot Predecode Duration",
                description="Duration of pre-decode when applying region snapshot",
                y_format="s",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="snapshot_predecode"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Snapshot Prehandle Throughput Heatmap",
                y_format="bytes",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_command_throughput_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="prehandle_snapshot"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
            make_heatmap(
                title="Snapshot Flush Duration",
                description="Duration of pre-decode when applying region snapshot",
                y_format="s",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="snapshot_flush"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    layout.row(
        [
            make_heatmap(
                title="Ingest Uncommitted Size Heatmap",
                y_format="bytes",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_write_flow_bytes_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"ingest_uncommitted"}[$__rate_interval])) by (le, type)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
            make_heatmap(
                title="Snapshot Predecode SST to DT Duration",
                description="Duration of SST to DT in pre-decode when applying region snapshot",
                y_format="s",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="snapshot_predecode_sst2dt"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    layout.half_row(
        [
            make_heatmap(
                title="Ingest SST Duration",
                description="Duration of ingesting SST",
                y_format="s",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_raft_command_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="ingest_sst"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def RoughSetFilterRateHistogram() -> RowPanel:
    layout = Layout(title="Rough Set Filter Rate Histogram")
    layout.row(
        [
            graph_panel(
                title="Rough Set Filter Rate",
                targets=[
                    target(
                        expr='avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]) - rate(tiflash_system_profile_event_DMFileFilterAftRoughSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) / (rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (instance)',
                        legend_format="1min-{{instance}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='avg((rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]) - rate(tiflash_system_profile_event_DMFileFilterAftRoughSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m])) / (rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[5m]))) by (instance)',
                        legend_format="5min-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_DMFileFilterNoFilter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="No Filter-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_DMFileFilterAftPKAndPackSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="PK Filter-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_DMFileFilterAftRoughSet{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="RS Filter-{{instance}}",
                        hide=True,
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit",
                    right_format="short",
                    left_min="0",
                    right_show=True,
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                series_overrides=[
                    {"alias": "/^RS Filter/", "yaxis": 2},
                    {"alias": "/^PK/", "yaxis": 2},
                    {"alias": "/^No Filter/", "yaxis": 2},
                ],
            ),
            make_heatmap(
                title="Rough Set Filter Rate Histogram",
                y_format="percent",
                log_base=1,
                hide_zero_buckets=True,
                max_data_points=512,
                targets=[
                    target(
                        expr='sum(delta(tiflash_storage_rough_set_filter_rate_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le)',
                        legend_format="{{le}}",
                        interval_factor=2,
                    ),
                ],
            ),
        ]
    )
    return layout.row_panel


def DisaggregatedWrite() -> RowPanel:
    layout = Layout(title="Disaggregated-Write")
    layout.row(
        [
            graph_panel(
                title="Checkpoint Upload Duration",
                description="PageStorage Checkpoint Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_checkpoint_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="{{type}}-max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_checkpoint_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_checkpoint_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_checkpoint_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_checkpoint_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_checkpoint_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_checkpoint_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="{{type}}-avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Checkpoint Upload flow",
                description="The flow of checkpoint operations",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_checkpoint_flow{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="incremental"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="incremental {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_storage_checkpoint_flow{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="compaction"}[$__rate_interval])) by ($additional_groupby)',
                        legend_format="compaction {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="binBps", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Checkpoint Upload keys speed by type (all)",
                description="The keys of checkpoint operations. All keys are uploaded in the checkpoint. Grouped by key types.",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_checkpoint_keys_by_types{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="none", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Checkpoint Upload flow by type (incremental+compaction)",
                description="The flow of checkpoint operations. Group by key types",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_checkpoint_flow_by_types{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="binBps", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Remote File Num",
                description="The number of files of owned by each TiFlash node",
                targets=[
                    target(
                        expr='sum(tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="num_files"}) by (instance)',
                        legend_format="checkpoint_data-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="short", right_format="percentunit", left_min="0"
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
            ),
            graph_panel(
                title="Remote Store Usage",
                description="The remote store usage owned by each TiFlash node",
                targets=[
                    target(
                        expr='sum(tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="total_size"}) by (instance)',
                        legend_format="remote_size-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="valid_size"}) by (instance)',
                        legend_format="valid_size-{{instance}}",
                    ),
                    target(
                        expr='sum((tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="valid_size"}) / (tiflash_storage_remote_stats{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="total_size"})) by (instance)',
                        legend_format="valid_rate-{{instance}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(
                    left_format="bytes",
                    right_format="percentunit",
                    left_min="0",
                    right_min="0",
                    right_max="1.1",
                    right_show=True,
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
                series_overrides=[
                    {"alias": "/^valid_rate/", "yaxis": 2},
                    {"alias": "/size/", "linewidth": 3},
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Remote Object Lock Request QPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_disaggregated_object_lock_request_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Remote Object Lock Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_disaggregated_object_lock_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_disaggregated_object_lock_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_disaggregated_object_lock_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_disaggregated_object_lock_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_disaggregated_object_lock_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_disaggregated_object_lock_request_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_disaggregated_object_lock_request_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Remote Store Summary",
                targets=[
                    target(
                        expr='sum(tiflash_storage_s3_store_summary_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, store_id, type)',
                        legend_format="store-{{store_id}}-{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="bytes", right_format="short", left_min="0"),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
            ),
            graph_panel(
                title="Remote GC Duration Breakdown",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_s3_gc_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_s3_gc_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_s3_gc_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_s3_gc_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_s3_gc_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_s3_gc_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_s3_gc_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(
                    left_format="s", right_format="short", left_min="0", right_show=True
                ),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
                series_overrides=[
                    {"alias": "/total/", "yaxis": 2},
                    {"alias": "/one_store/", "yaxis": 2},
                    {"alias": "/clean_locks/", "yaxis": 2},
                ],
            ),
            graph_panel(
                title="Remote GC Status",
                targets=[
                    target(
                        expr='sum(tiflash_storage_s3_gc_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short", left_min="0"),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Local Lock Manager status",
                targets=[
                    target(
                        expr='sum(tiflash_storage_s3_lock_mgr_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short", left_min="0"),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
            ),
            graph_panel(
                title="Local Lock Manager QPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_s3_lock_mgr_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="FAP result",
                targets=[
                    target(
                        expr='sum(rate(tiflash_fap_task_result{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="percentunit",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                series_overrides=[{"alias": "/hit_ratio/", "yaxis": 2}],
            ),
            graph_panel(
                title="FAP state",
                targets=[
                    target(
                        expr='sum(rate(tiflash_fap_task_state{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="percentunit",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                series_overrides=[{"alias": "/hit_ratio/", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="FAP time by stage",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_fap_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="{{type}}-max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_fap_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_fap_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_fap_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_fap_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_fap_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_fap_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="{{type}}-avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(
                    left_format="s", right_format="short", left_min="0", right_show=True
                ),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
                series_overrides=[{"alias": "/hit_ratio/", "yaxis": 2}],
            ),
            graph_panel(
                title="FAP no match reason",
                targets=[
                    target(
                        expr='sum(rate(tiflash_fap_nomatch_reason{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="percentunit",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                series_overrides=[{"alias": "/hit_ratio/", "yaxis": 2}],
            ),
        ]
    )
    return layout.row_panel


def DisaggregatedCompute() -> RowPanel:
    layout = Layout(title="Disaggregated-Compute")
    layout.row(
        [
            graph_panel(
                title="Read Duration Breakdown",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_disaggregated_breakdown_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_disaggregated_breakdown_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_disaggregated_breakdown_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_disaggregated_breakdown_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_disaggregated_breakdown_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_disaggregated_breakdown_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_disaggregated_breakdown_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Remote Cache Operations",
                description="Remote Cache Operations",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dtfile_hit"}[$__rate_interval])) / sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"dtfile_hit|dtfile_miss"}[$__rate_interval])))',
                        legend_format="dtfile_cache_hit_ratio",
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"page_hit"}[$__rate_interval])) / sum(rate(tiflash_storage_remote_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"page_hit|page_miss"}[$__rate_interval])))',
                        legend_format="page_cache_hit_ratio",
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="percentunit",
                    left_min="0",
                    right_show=True,
                ),
                fill_gradient=0,
                series_overrides=[
                    {"alias": "dtfile_cache_hit_ratio", "yaxis": 2},
                    {"alias": "page_cache_hit_ratio", "yaxis": 2},
                ],
            ),
            graph_panel(
                title="Remote Cache Flow",
                description="Remote Cache Flow",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_remote_cache_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="binBps", right_format="percentunit", left_min="0"
                ),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Remote Cache BG Download Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, stage, file_type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{stage}}-{{file_type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, stage, file_type, $additional_groupby))',
                        legend_format="9999-{{stage}}-{{file_type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, stage, file_type, $additional_groupby))',
                        legend_format="999-{{stage}}-{{file_type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, stage, file_type, $additional_groupby))',
                        legend_format="99-{{stage}}-{{file_type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, stage, file_type, $additional_groupby))',
                        legend_format="80-{{stage}}-{{file_type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (stage, file_type, $additional_groupby) / sum(rate(tiflash_storage_remote_cache_bg_download_stage_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (stage, file_type, $additional_groupby))',
                        legend_format="avg-{{stage}}-{{file_type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Remote Cache Wait on Downloading Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, result, file_type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{result}}-{{file_type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, result, file_type, $additional_groupby))',
                        legend_format="9999-{{result}}-{{file_type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, result, file_type, $additional_groupby))',
                        legend_format="999-{{result}}-{{file_type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, result, file_type, $additional_groupby))',
                        legend_format="99-{{result}}-{{file_type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, result, file_type, $additional_groupby))',
                        legend_format="80-{{result}}-{{file_type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (result, file_type, $additional_groupby) / sum(rate(tiflash_storage_remote_cache_wait_on_downloading_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (result, file_type, $additional_groupby))',
                        legend_format="avg-{{result}}-{{file_type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Remote Cache Wait on Downloading OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_remote_cache_wait_on_downloading_result{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (result, file_type, $additional_groupby)',
                        legend_format="{{result}}-{{file_type}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="s",
                    left_min="0",
                    left_decimals=0,
                    right_show=True,
                ),
                fill=1,
                fill_gradient=0,
                series_overrides=[{"alias": "", "yaxis": 2}],
            ),
            graph_panel(
                title="Remote Cache Wait on Downloading Flow",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_remote_cache_wait_on_downloading_bytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (result, file_type, $additional_groupby)',
                        legend_format="{{result}}-{{file_type}} {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="binBps", right_format="percentunit", left_min="0"
                ),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Remote Cache Gauge",
                targets=[
                    target(
                        expr='sum(tiflash_storage_remote_cache_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type, instance)',
                        legend_format="{{type}}-{{instance}}",
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short"),
                fill=1,
                fill_gradient=0,
            ),
            graph_panel(
                title="Remote Cache Reject Download Type OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_remote_cache_reject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (reason, file_type, $additional_groupby)',
                        legend_format="{{reason}}-{{file_type}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="s",
                    left_min="0",
                    left_decimals=0,
                    right_show=True,
                ),
                fill=1,
                fill_gradient=0,
                series_overrides=[{"alias": "", "yaxis": 2}],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Remote Cache Usage",
                description="Remote Cache Usage",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_DTFileCacheCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="DTFileCapacity-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_DTFileCacheUsed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="DTFileUsed-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_PageCacheCapacity{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="PageCapacity-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_PageCacheUsed{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="PageUsed-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="bytes", right_format="percentunit", left_min="0"
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
            ),
            graph_panel(
                title="Memory Usage of Storage Tasks",
                description="Memory Usage of Storage Tasks",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_MemoryTrackingQueryStorageTask{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="MemoryTrackingQueryStorageTask-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_MemoryTrackingFetchPages{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="MemoryTrackingFetchPages-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_DT_DeltaIndexCacheSize{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="DeltaIndexCacheSize-{{instance}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_MemoryTrackingSharedColumnData{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="SharedColumnData-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="bytes", right_format="percentunit", left_min="0"
                ),
                legend=graph_legend(current=False),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="MVCCIndexCache",
                description="DeltaIndex cache of ReadNodes",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_mvcc_index_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, instance)',
                        legend_format="{{type}}-{{instance}}",
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_mvcc_index_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"hit"}[$__rate_interval])) by (instance) / sum(rate(tiflash_storage_mvcc_index_cache{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance))',
                        legend_format="hit_ratio-{{instance}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops",
                    right_format="percentunit",
                    left_min="0",
                    right_show=True,
                ),
                fill_gradient=0,
                series_overrides=[{"alias": "/hit_ratio/", "yaxis": 2}],
            ),
            graph_panel(
                title="PlaceIndex Tasks Duration",
                description="Duration of storage's internal sub tasks",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_subtask_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_subtask_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_subtask_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="place_index_update"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="PlaceIndexTask/Reuse OPS",
                description="Total number of storage's internal sub tasks",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_place_index_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_storage_subtask_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~"place_index_update"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="{{type}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="ops", right_format="opm", left_min="0", left_decimals=1
                ),
                fill_gradient=0,
            ),
            graph_panel(
                title="PlaceIndex update rows/deletes",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_place_index_stats_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, $additional_groupby) / 1000000000)',
                        legend_format="max {{$additional_groupby}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_place_index_stats_count_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                        hide=True,
                        interval_factor=1,
                    ),
                    target(
                        expr='sum by (type, $additional_groupby) (rate(tiflash_storage_place_index_stats_count_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) / sum by (type, $additional_groupby) (rate(tiflash_storage_place_index_stats_count_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="opm", left_min="0"),
                fill=1,
                fill_gradient=0,
            ),
        ]
    )
    return layout.row_panel


def S3() -> RowPanel:
    layout = Layout(title="S3")
    layout.row(
        [
            graph_panel(
                title="S3 Bytes",
                description="S3 read/write throughput",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3WriteBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3WriteBytes {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3ReadBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3ReadBytes {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3WriteDMFileBytes{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3WriteDMFileBytes {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="binBps", right_format="opm", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="S3 OPS",
                description="S3 OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3PutObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3PutObject {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3GetObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3GetObject {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3HeadObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3HeadObject {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3ListObjects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3ListObjects {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3DeleteObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3DeleteObject {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3CopyObject{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3CopyObject {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3CreateMultipartUpload{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3CreateMultipartUpload {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3UploadPart{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3UploadPart {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3CompleteMultipartUpload{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3CompleteMultipartUpload {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3PutDMFile{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3PutDMFile {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IORead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IORead {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IOSeek{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IOSeek {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="S3 Retry OPS",
                description="S3 Retry OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3GetObjectRetry{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3GetObjectRetry {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3PutObjectRetry{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3PutObjectRetry {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3PutDMFileRetry{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3PutDMFileRetry {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IOReadError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IOReadError {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IOSeekError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IOSeekError {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IOSeekBackward{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IOSeekBackward {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="S3 Request Duration",
                description="S3 Request Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_s3_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="{{type}}-max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_s3_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_s3_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_s3_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_s3_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_s3_request_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_s3_request_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="{{type}}-avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="S3 HTTP OPS",
                description="S3 HTTP OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3ReadRequestsCount{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="read-count {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3WriteRequestsCount{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="write-count {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3ReadRequestsErrors{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="read-error {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3WriteRequestsErrors{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="write-error {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3ReadRequestsThrottling{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="read-throttling {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3WriteRequestsThrottling{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="write-throttling {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3ReadRequestsRedirects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="read-redirects {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3WriteRequestsRedirects{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="write-redirects {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3ReadRequestsNotFound{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="read-notfound {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3WriteRequestsNotFound{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="write-notfound {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="S3 HTTP Request Duration",
                description="S3 HTTP Request Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_storage_s3_http_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="{{type}}-max {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_storage_s3_http_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-9999 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_storage_s3_http_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-999 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_storage_s3_http_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-99 {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_storage_s3_http_request_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="{{type}}-80 {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_storage_s3_http_request_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_storage_s3_http_request_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="{{type}}-avg {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="S3 on-going instances",
                description="S3 HTTP OPS",
                targets=[
                    target(
                        expr='sum(tiflash_system_current_metric_S3Requests{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type, $additional_groupby)',
                        legend_format="S3Requests {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(tiflash_system_current_metric_S3RandomAccessFile{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type, $additional_groupby)',
                        legend_format="S3RandomAccessFile {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="opm", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="S3RandomAccessFile OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IOReadError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IOReadError {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IOSeekError{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IOSeekError {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IOSeekBackward{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IOSeekBackward {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IORead{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IORead {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(rate(tiflash_system_profile_event_S3IOSeek{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby)',
                        legend_format="S3IOSeek {{$additional_groupby}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="opm", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    return layout.row_panel


def PipelineModel() -> RowPanel:
    layout = Layout(title="Pipeline Model")
    layout.row(
        [
            graph_panel(
                title="Task Thread Pool Size",
                targets=[
                    target(
                        expr='max(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_task_thread_pool_size"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
            graph_panel(
                title="Task Count",
                targets=[
                    target(
                        expr='max(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_tasks_count"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_pipeline_scheduler{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type=~".*_tasks_count"}) by (type)',
                        legend_format="sum({{type}})",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Task Status Change OPS",
                targets=[
                    target(
                        expr='sum(rate(tiflash_pipeline_task_change_to_status{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type)',
                        legend_format="{{type}}",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="none", left_min="0"),
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Task Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_pipeline_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type, $additional_groupby) / 1000000000)',
                        legend_format="max-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_pipeline_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="9999-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_pipeline_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="999-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_pipeline_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="99-{{type}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_pipeline_task_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type, $additional_groupby))',
                        legend_format="80-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (type, $additional_groupby))',
                        legend_format="avg-{{type}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu_execute"}[$__rate_interval])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu_execute"}[$__rate_interval])))',
                        legend_format="avg-cpu_execute",
                    ),
                    target(
                        expr='(sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu_queue"}[$__rate_interval])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu_queue"}[$__rate_interval])))',
                        legend_format="avg-cpu_queue",
                    ),
                    target(
                        expr='(sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io_execute"}[$__rate_interval])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io_execute"}[$__rate_interval])))',
                        legend_format="avg-io_execute",
                    ),
                    target(
                        expr='(sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io_queue"}[$__rate_interval])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io_queue"}[$__rate_interval])))',
                        legend_format="avg-io_queue",
                    ),
                    target(
                        expr='(sum(rate(tiflash_pipeline_task_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="await"}[$__rate_interval])) / sum(rate(tiflash_pipeline_task_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="await"}[$__rate_interval])))',
                        legend_format="avg-await",
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Task Max Execute Time Per Round",
                targets=[
                    target(
                        expr='histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type))',
                        legend_format="95-{{type}}",
                    ),
                    target(
                        expr='histogram_quantile(0.95, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type))',
                        legend_format="99-{{type}}",
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, type))',
                        legend_format="999-{{type}}",
                    ),
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, type) / 1000000000)',
                        legend_format="100-{{type}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu"}[$__rate_interval])) / sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="cpu"}[$__rate_interval]))',
                        legend_format="avg-cpu",
                    ),
                    target(
                        expr='sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io"}[$__rate_interval])) / sum(rate(tiflash_pipeline_task_execute_max_time_seconds_per_round_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="io"}[$__rate_interval]))',
                        legend_format="avg-io",
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
            ),
            graph_panel(
                title="Threads CPU of CPU Task Thread Pool",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"cpu_pool", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"cpu_pool", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="Threads CPU of IO Task Thread Pool",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"io_pool", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"io_pool", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
            graph_panel(
                title="Threads CPU of Wait Reactor",
                targets=[
                    target(
                        expr='sum(rate(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"WaitReactor", instance=~"$tiflash_role"}[$__rate_interval])) by (instance)',
                        legend_format="{{name}} {{instance}}",
                    ),
                    target(
                        expr='count(tiflash_proxy_thread_cpu_seconds_total{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"WaitReactor", instance=~"$tiflash_role"}) by (instance)',
                        legend_format="Limit",
                    ),
                ],
                yaxes=yaxes(
                    left_format="percentunit", right_format="short", left_min="0"
                ),
                fill_gradient=0,
                null_point_mode="null",
                series_overrides=[
                    {
                        "alias": "Limit",
                        "color": "#F2495C",
                        "hideTooltip": True,
                        "legend": False,
                        "linewidth": 2,
                        "nullPointMode": "connected",
                    }
                ],
            ),
        ]
    )
    layout.half_row(
        [
            graph_panel(
                title="Wait notify task details",
                description="wait notify task details",
                targets=[
                    target(
                        expr='max(tiflash_pipeline_wait_on_notify_tasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (instance, type)',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='sum(tiflash_pipeline_wait_on_notify_tasks{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"}) by (type)',
                        legend_format="sum({{type}})",
                    ),
                ],
                yaxes=yaxes(left_format="none", right_format="short", left_min="0"),
                fill_gradient=0,
            ),
        ]
    )
    return layout.row_panel


def TiFlashResourceControl() -> RowPanel:
    layout = Layout(title="TiFlash Resource Control")
    layout.row(
        [
            graph_panel(
                title="TiFlash Resource Group",
                description="Metas of resource group",
                targets=[
                    target(
                        expr='max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="remaining_tokens", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
                        legend_format="remaining_tokens-{{instance}}-{{resource_group}}",
                    ),
                    target(
                        expr='max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="avg_speed", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
                        legend_format="avg_speed-{{instance}}-{{resource_group}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="total_consumption", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, resource_group)',
                        legend_format="total_consumption-{{instance}}-{{resource_group}}",
                        hide=True,
                    ),
                    target(
                        expr='max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="bucket_fill_rate", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
                        legend_format="bucket_fill_rate-{{instance}}-{{resource_group}}",
                        hide=True,
                    ),
                    target(
                        expr='max(tiflash_resource_group{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="bucket_capacity", instance=~"$instance", instance=~"$tiflash_role"}) by (instance,resource_group)',
                        legend_format="bucket_capacity-{{instance}}-{{resource_group}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="request_gac_count", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, resource_group)',
                        legend_format="request_gac_count-{{instance}}-{{resource_group}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="gac_req_ru_consumption_delta", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, resource_group)',
                        legend_format="gac_req_ru_consumption_delta-{{instance}}-{{resource_group}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="compute_ru_consumption", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, resource_group)',
                        legend_format="compute_ru_consumption-{{instance}}-{{resource_group}}",
                        hide=True,
                    ),
                    target(
                        expr='sum(rate(tiflash_resource_group_counter{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", type="storage_ru_consumption", instance=~"$instance", instance=~"$tiflash_role"}[$__rate_interval])) by (instance, resource_group)',
                        legend_format="storage_ru_consumption-{{instance}}-{{resource_group}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="short", right_format="short"),
                legend=graph_legend(avg=True, max=False),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
            graph_panel(
                title="Request Unit",
                description="Request Unit for tidb-serverless charging",
                targets=[
                    target(
                        expr='sum(rate(tiflash_storage_sync_replica_ru{instance=~"$tiflash_role"}[$__rate_interval])) by (keyspace_id, $additional_groupby)',
                        legend_format="replica-sync-rate-{{keyspace_id}}",
                    ),
                    target(
                        expr='sum(increase(tiflash_storage_sync_replica_ru{instance=~"$tiflash_role"}[24h])) by (keyspace_id, $additional_groupby)',
                        legend_format="replica-sync-sum-24h-{{keyspace_id}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_compute_request_unit{instance=~"$tiflash_role"}[$__rate_interval])) by (cluster_id, $additional_groupby)',
                        legend_format="query-rate-{{cluster_id}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(increase(tiflash_compute_request_unit{instance=~"$tiflash_role"}[24h])) by (cluster_id, $additional_groupby)',
                        legend_format="query-sum-24h-{{cluster_id}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='sum(rate(tiflash_storage_ru_read_bytes{instance=~"$tiflash_role"}[$__rate_interval])) by (keyspace, resource_group, type, $additional_groupby) / (64 * 1024)',
                        legend_format="storage-{{keyspace}}_{{resource_group}}_{{type}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(
                    left_format="cps",
                    right_format="short",
                    left_min="0",
                    right_min="0",
                    right_show=True,
                ),
                legend=graph_legend(max=False),
                fill_gradient=0,
                decimals=1,
                series_overrides=[{"alias": "/sum/", "yaxis": 2}],
            ),
        ]
    )
    return layout.row_panel


def StatusServer() -> RowPanel:
    layout = Layout(title="Status Server")
    layout.row(
        [
            graph_panel(
                title="Status API Request Duration",
                targets=[
                    target(
                        expr='histogram_quantile(1.00, sum(round(1000000000*rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval]))) by (le, path, $additional_groupby) / 1000000000)',
                        legend_format="max-{{path}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.9999, sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, path, $additional_groupby))',
                        legend_format="9999-{{path}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.999, sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, path, $additional_groupby))',
                        legend_format="999-{{path}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='histogram_quantile(0.99, sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, path, $additional_groupby))',
                        legend_format="99-{{path}} {{$additional_groupby}}",
                    ),
                    target(
                        expr='histogram_quantile(0.80, sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_bucket{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (le, path, $additional_groupby))',
                        legend_format="80-{{path}} {{$additional_groupby}}",
                        hide=True,
                    ),
                    target(
                        expr='(sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_sum{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (path, $additional_groupby) / sum(rate(tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_count{k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"}[$__rate_interval])) by (path, $additional_groupby))',
                        legend_format="avg-{{path}} {{$additional_groupby}}",
                        hide=True,
                    ),
                ],
                yaxes=yaxes(left_format="s", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                tooltip_sort=2,
            ),
            graph_panel(
                title="Status API Request (op/s)",
                targets=[
                    target(
                        expr='sum(rate( tiflash_proxy_tikv_status_server_proxy_request_duration_seconds_count {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"} [$__rate_interval] )) by (path, $additional_groupby)',
                        legend_format="{{path}} {{$additional_groupby}}",
                    ),
                ],
                yaxes=yaxes(left_format="ops", right_format="short", left_min="0"),
                fill=1,
                fill_gradient=0,
                null_point_mode="null",
            ),
        ]
    )
    return layout.row_panel


def VectorSearch() -> RowPanel:
    layout = Layout(title="Vector Search")
    layout.row(
        [
            graph_panel(
                title="In-Memory Vector Index Instances",
                targets=[
                    target(
                        expr='sum by (type, instance) ( tiflash_vector_index_active_instances{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role" } )',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="short",
                    right_format="ops",
                    left_min="0",
                    left_decimals=0,
                ),
                legend=graph_legend(current=False),
                fill_gradient=0,
                decimals=0,
            ),
            graph_panel(
                title="Vector Index Estimated Memory Usage",
                targets=[
                    target(
                        expr='tiflash_vector_index_memory_usage{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role" }',
                        legend_format="{{instance}}-{{type}}",
                        interval_factor=1,
                    ),
                    target(
                        expr='tiflash_process_rss_by_type_bytes{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="file" }',
                        legend_format="{{instance}}-RssFile",
                    ),
                ],
                yaxes=yaxes(
                    left_format="bytes",
                    right_format="ops",
                    left_min="0",
                    left_decimals=0,
                ),
                legend=graph_legend(current=False),
                fill_gradient=0,
                decimals=0,
            ),
        ]
    )
    layout.row(
        [
            graph_panel(
                title="99.9% Vector Search Duration (Per Request)",
                targets=[
                    target(
                        expr='histogram_quantile( 0.999, sum(rate( tiflash_vector_index_duration_bucket{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type!="build" } [$__rate_interval] )) by (le, type) )',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="s",
                    right_format="s",
                    left_min="0",
                    left_decimals=1,
                    right_min="0",
                    right_decimals=1,
                    right_show=True,
                ),
                legend=graph_legend(current=False),
                fill_gradient=0,
                decimals=1,
                series_overrides=[{"alias": "/download/", "yaxis": 2}],
            ),
            graph_panel(
                title="99.9% Vector Index Build Duration (Per DMFile Column)",
                targets=[
                    target(
                        expr='histogram_quantile( 0.999, sum(rate( tiflash_vector_index_duration_bucket{ k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role", type="build" } [$__rate_interval] )) by (le, type) )',
                        legend_format="{{type}}",
                        interval_factor=1,
                    ),
                ],
                yaxes=yaxes(
                    left_format="s", right_format="s", left_min="0", left_decimals=1
                ),
                legend=graph_legend(current=False),
                fill_gradient=0,
                decimals=1,
            ),
        ]
    )
    return layout.row_panel


dashboard = Dashboard(
    title="Test-Cluster-TiFlash-Summary",
    uid="SVbh2xUWk",
    timezone="browser",
    refresh="1m",
    inputs=[DATASOURCE_INPUT],
    editable=True,
    templating=Templates(),
    panels=[
        Server(),
        ThreadsCPU(),
        Threads(),
        Coprocessor(),
        TaskScheduler(),
        DDL(),
        ImbalanceReadWrite(),
        MemoryTrace(),
        ColumnarStorage(),
        Storage(),
        StorageReadPoolDataSharing(),
        PageStorage(),
        RateLimiter(),
        StorageWriteStall(),
        Raft(),
        RaftSnapshotIngestSST(),
        RoughSetFilterRateHistogram(),
        DisaggregatedWrite(),
        DisaggregatedCompute(),
        S3(),
        PipelineModel(),
        TiFlashResourceControl(),
        StatusServer(),
        VectorSearch(),
    ],
    # Set 14 or larger to support shared crosshair or shared tooltip.
    # See https://github.com/grafana/grafana/blob/v10.2.2/public/app/features/dashboard/state/DashboardMigrator.ts#L443-L445
    schemaVersion=14,
    graphTooltip=GRAPH_TOOLTIP_MODE_SHARED_CROSSHAIR,
).auto_panel_ids()
