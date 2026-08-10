from __future__ import annotations

import contextvars
import re
from contextlib import contextmanager
from typing import Optional, Sequence, Union

import attr
from attr.validators import in_, instance_of
from grafanalib import formatunits as UNITS
from grafanalib.core import (
    NULL_AS_ZERO,
    TIME_SERIES_TARGET_FORMAT,
    DataSourceInput,
    Graph,
    GraphThreshold,
    GridPos,
    Heatmap,
    HeatmapColor,
    Legend,
    Panel,
    RowPanel,
    SeriesOverride,
    Stat,
    StatValueMappings,
    Target,
    Template,
    TimeSeries,
    Tooltip,
    YAxes,
    YAxis,
)

DATASOURCE_INPUT = DataSourceInput(
    name="DS_TEST-CLUSTER",
    label="Test-Cluster",
    pluginId="prometheus",
    pluginName="Prometheus",
)
DATASOURCE = f"${{{DATASOURCE_INPUT.name}}}"
ADDITIONAL_GROUPBY = "$additional_groupby"
OPTIONAL_QUANTILE = "optional_quantile"
OPTIONAL_QUANTILE_INPUT = "$" + OPTIONAL_QUANTILE

_TIDB_CLUSTER_SELECTOR_RE = re.compile(r"^\s*tidb_cluster(_id)?\s*(=~|=|!=|!~).*$")

CLUSTER_LABEL_SELECTORS = (
    r'k8s_cluster="$k8s_cluster"',
    r'tidb_cluster="$tidb_cluster"',
)
CPP_LABEL_SELECTORS = (
    r'instance=~"$instance"',
    r'instance=~"$tiflash_role"',
)
PROXY_LABEL_SELECTORS = (
    r'instance=~"$proxy_instance"',
    r'instance=~"$tiflash_role"',
)


@attr.s(frozen=True)
class PromQLPolicy:
    """Implicit label selectors applied while constructing PromQL expressions."""

    default_label_selectors: tuple[str, ...] = attr.ib(converter=tuple)
    shared_pool_selector: Optional[str] = attr.ib(default=None)


STANDARD_PROMQL_POLICY = PromQLPolicy(
    default_label_selectors=CLUSTER_LABEL_SELECTORS + CPP_LABEL_SELECTORS,
)
PROXY_PROMQL_POLICY = PromQLPolicy(
    default_label_selectors=CLUSTER_LABEL_SELECTORS + PROXY_LABEL_SELECTORS,
)
# Alias kept for callers that only need cluster+cpp defaults.
SERVERLESS_PROMQL_POLICY = STANDARD_PROMQL_POLICY
_active_promql_policy = contextvars.ContextVar(
    "active_promql_policy", default=STANDARD_PROMQL_POLICY
)


def instance_selectors_policy(instance_selectors: str = "cpp") -> PromQLPolicy:
    """Return policy for cpp (default) or proxy instance label selectors."""
    if instance_selectors == "proxy":
        return PROXY_PROMQL_POLICY
    if instance_selectors == "cpp":
        return STANDARD_PROMQL_POLICY
    raise ValueError(
        f"unknown instance_selectors={instance_selectors!r}; use 'cpp' or 'proxy'"
    )


@contextmanager
def use_instance_selectors(instance_selectors: str = "cpp"):
    """Select cpp_label_selectors or proxy_label_selectors for Expr construction."""
    with use_promql_policy(instance_selectors_policy(instance_selectors)):
        yield


@contextmanager
def use_promql_policy(policy: PromQLPolicy):
    """Use a PromQL policy for expressions created within this context."""

    token = _active_promql_policy.set(policy)
    try:
        yield
    finally:
        _active_promql_policy.reset(token)


@attr.s
class Expr(object):
    """
    A prometheus expression that matches the following grammar:

    expr ::= <aggr_op> (
                [aggr_param,]
                [func](
                    <metric name>
                    [{<label_selectors>,}]
                    [[<range_selector>]]
                )
            ) [by (<by_labels>,)] [extra_expr]
    """

    def metric_validator(instance, attribute, value):
        if not isinstance(value, (str, Expr)):
            raise TypeError(
                f"'{attribute.name}' must be an instance of 'str' or 'Expr'"
            )

    metric: Union[str, Expr] = attr.ib(validator=metric_validator)
    aggr_op: str = attr.ib(
        default="",
        validator=in_(
            [
                "",
                "sum",
                "min",
                "max",
                "avg",
                "group",
                "stddev",
                "stdvar",
                "count",
                "count_values",
                "bottomk",
                "topk",
                "quantile",
            ]
        ),
    )
    aggr_param: str = attr.ib(default="", validator=instance_of(str))
    func: str = attr.ib(default="", validator=instance_of(str))
    range_selector: str = attr.ib(default="", validator=instance_of(str))
    label_selectors: list[str] = attr.ib(default=[], validator=instance_of(list))
    by_labels: list[str] = attr.ib(default=[], validator=instance_of(list))
    default_label_selectors: list[str] = attr.ib(
        factory=lambda: list(CLUSTER_LABEL_SELECTORS),
        validator=instance_of(list),
    )
    instance_selector: tuple = attr.ib(
        factory=lambda: tuple(CPP_LABEL_SELECTORS),
        converter=tuple,
    )
    shared_pool_selector: Optional[str] = attr.ib(
        factory=lambda: _active_promql_policy.get().shared_pool_selector,
    )
    use_shared_pool: bool = attr.ib(default=False, validator=instance_of(bool))
    extra_expr: str = attr.ib(default="", validator=instance_of(str))

    def __str__(self) -> str:
        aggr_opeator = self.aggr_op if self.aggr_op else ""
        aggr_param = self.aggr_param + "," if self.aggr_param else ""
        by_clause = (
            "by ({})".format(", ".join(self.by_labels)) if self.by_labels else ""
        )
        func = self.func if self.func else ""
        # CLUSTER (+ optional cleared defaults) + instance_selector + extra label_selectors.
        label_selectors = (
            list(self.default_label_selectors)
            + list(self.instance_selector)
            + list(self.label_selectors)
        )
        if self.use_shared_pool:
            label_selectors = [
                l for l in label_selectors if not _TIDB_CLUSTER_SELECTOR_RE.match(l)
            ]
            if self.shared_pool_selector is not None:
                label_selectors.append(self.shared_pool_selector)

        assert all(
            ("=" in item or "~" in item) for item in label_selectors
        ), f"Not all items contain '=' or '~', invalid {self.label_selectors}"
        instant_selectors = (
            "{{{}}}".format(",".join(label_selectors)) if label_selectors else ""
        )
        range_selector = f"[{self.range_selector}]" if self.range_selector else ""
        extra_expr = self.extra_expr if self.extra_expr else ""
        return f"""{aggr_opeator}({aggr_param}{func}(
    {self.metric}
    {instant_selectors}
    {range_selector}
)) {by_clause} {extra_expr}"""

    def aggregate(
        self,
        aggr_op: str,
        aggr_param: str = "",
        by_labels: list[str] = [],
        label_selectors: list[str] = [],
    ) -> "Expr":
        self.aggr_op = aggr_op
        self.aggr_param = aggr_param
        self.by_labels = by_labels
        self.label_selectors = label_selectors
        return self

    def function(
        self,
        func: str,
        label_selectors: list[str] = [],
        range_selector: str = "",
    ) -> "Expr":
        self.func = func
        self.label_selectors = label_selectors
        self.range_selector = range_selector
        return self

    def extra(
        self,
        extra_expr: Optional[str] = None,
        default_label_selectors: Optional[list[str]] = None,
    ) -> "Expr":
        if extra_expr is not None:
            self.extra_expr = extra_expr
        if default_label_selectors is not None:
            self.default_label_selectors = default_label_selectors
        return self

    def use_shared_pool_selector(self) -> "Expr":
        self.use_shared_pool = True
        return self

    def append_by_labels(self, label: str) -> "Expr":
        if isinstance(self.metric, Expr):
            # append the label to the inner expr
            self.metric.append_by_labels(label)
        else:
            self.by_labels = self.by_labels + [label]
        return self


class OpExpr:
    lhs: Union[Expr, OpExpr, str]
    op: str
    rhs: Union[Expr, OpExpr, str]

    def __init__(
        self, lhs: Union[Expr, OpExpr, str], op: str, rhs: Union[Expr, OpExpr, str]
    ):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def __str__(self) -> str:
        return f"""({self.lhs} {self.op} {self.rhs})"""

    def __repr__(self) -> str:
        return self.__str__()

    def append_by_labels(self, label: str) -> "OpExpr":
        if not isinstance(self.lhs, str):
            self.lhs.append_by_labels(label)
        if not isinstance(self.rhs, str):
            self.rhs.append_by_labels(label)
        return self


def expr_aggr(
    metric: Union[str, Expr],
    aggr_op: str,
    aggr_param: str = "",
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
    use_shared_pool: bool = False,
) -> Expr:
    """
    Calculate the aggregation of a metric.

    Example:

        sum((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    expr = Expr(metric=metric)
    expr.aggregate(
        aggr_op,
        aggr_param=aggr_param,
        by_labels=by_labels,
        label_selectors=label_selectors,
    )
    expr.instance_selector = tuple(instance_selector)
    expr.use_shared_pool = use_shared_pool
    return expr


def expr_sum(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the sum of a metric.

    Example:

        sum((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    return expr_aggr(
        metric,
        "sum",
        instance_selector=instance_selector,
        label_selectors=label_selectors,
        by_labels=by_labels,
    )


def expr_avg(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the avg of a metric.

    Example:

    avg((
        tikv_store_size_bytes
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
    )) by (instance)
    """
    return expr_aggr(
        metric,
        "avg",
        instance_selector=instance_selector,
        label_selectors=label_selectors,
        by_labels=by_labels,
    )


def expr_max(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the max of a metric.

    Example:

        max((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    return expr_aggr(
        metric,
        "max",
        instance_selector=instance_selector,
        label_selectors=label_selectors,
        by_labels=by_labels,
    )


def expr_min(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the min of a metric.

    Example:

        min((
            tikv_store_size_bytes
            {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        )) by (instance)
    """
    return expr_aggr(
        metric,
        "min",
        instance_selector=instance_selector,
        label_selectors=label_selectors,
        by_labels=by_labels,
    )


def expr_aggr_func(
    metric: str,
    aggr_op: str,
    func: str,
    aggr_param: str = "",
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    range_selector: str = "",
    by_labels: list[str] = ["instance"],
    use_shared_pool: bool = False,
) -> Expr:
    """
    Calculate the aggregation of function of a metric.

    Example:

    expr_aggr_func(
        tikv_grpc_msg_duration_seconds_count, "sum", "rate", lables_selectors=['type!="kv_gc"']
    )

    sum(rate(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (instance)
    """
    expr = Expr(metric=metric)
    expr.aggregate(
        aggr_op,
        aggr_param=aggr_param,
        by_labels=by_labels,
    )
    expr.function(
        func,
        label_selectors=label_selectors,
        range_selector=range_selector,
    )
    expr.instance_selector = tuple(instance_selector)
    expr.use_shared_pool = use_shared_pool
    return expr


def expr_sum_rate(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
    use_shared_pool: bool = False,
) -> Expr:
    """
    Calculate the sum of rate of a metric.

    Example:

    sum(rate(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (instance)
    """
    # $__rate_interval is a Grafana variable that is specialized for Prometheus
    # rate and increase function.
    # See https://grafana.com/blog/2020/09/28/new-in-grafana-7.2-__rate_interval-for-prometheus-rate-queries-that-just-work/
    return expr_aggr_func(
        metric=metric,
        aggr_op="sum",
        func="rate",
        label_selectors=label_selectors,
        range_selector="$__rate_interval",
        by_labels=by_labels,
        instance_selector=instance_selector,
        use_shared_pool=use_shared_pool,
    )


def expr_sum_irate(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
    use_shared_pool: bool = False,
) -> Expr:
    """sum(irate(metric{...}[$__rate_interval])) by (...)"""
    return expr_aggr_func(
        metric=metric,
        aggr_op="sum",
        func="irate",
        label_selectors=label_selectors,
        range_selector="$__rate_interval",
        by_labels=by_labels,
        instance_selector=instance_selector,
        use_shared_pool=use_shared_pool,
    )


def expr_sum_delta(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    range_selector: str = "$__rate_interval",
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the sum of delta of a metric.

    Example:

    sum(delta(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (instance)
    """
    return expr_aggr_func(
        metric=metric,
        aggr_op="sum",
        func="delta",
        label_selectors=label_selectors,
        range_selector=range_selector,
        by_labels=by_labels,
        instance_selector=instance_selector,
    )


def expr_sum_increase(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    range_selector: str = "$__rate_interval",
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the sum of increase of a metric.

    Example:

    sum(increase(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (instance)
    """
    return expr_aggr_func(
        metric=metric,
        aggr_op="sum",
        func="increase",
        instance_selector=instance_selector,
        label_selectors=label_selectors,
        range_selector=range_selector,
        by_labels=by_labels,
    )


def expr_sum_aggr_over_time(
    metric: str,
    aggr: str,
    range_selector: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the sum of average value of all points in the specified interval of a metric.

    Example:

    sum(avg_over_time(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [1m]
    )) by (instance)
    """
    return expr_aggr_func(
        metric=metric,
        aggr_op="sum",
        func=f"{aggr}_over_time",
        label_selectors=label_selectors,
        range_selector=range_selector,
        by_labels=by_labels,
        instance_selector=instance_selector,
    )


def expr_max_rate(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the max of rate of a metric.

    Example:

    max(rate(
        tikv_thread_voluntary_context_switches
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (name)
    """
    # $__rate_interval is a Grafana variable that is specialized for Prometheus
    # rate and increase function.
    # See https://grafana.com/blog/2020/09/28/new-in-grafana-7.2-__rate_interval-for-prometheus-rate-queries-that-just-work/
    return expr_aggr_func(
        metric=metric,
        aggr_op="max",
        func="rate",
        label_selectors=label_selectors,
        range_selector="$__rate_interval",
        by_labels=by_labels,
        instance_selector=instance_selector,
    )


def expr_count_rate(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> Expr:
    """
    Calculate the count of rate of a metric.

    Example:

    count(rate(
        tikv_thread_cpu_seconds_total
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",name=~"sst_.*"}
        [$__rate_interval]
    )) by (instance)
    """
    # $__rate_interval is a Grafana variable that is specialized for Prometheus
    # rate and increase function.
    # See https://grafana.com/blog/2020/09/28/new-in-grafana-7.2-__rate_interval-for-prometheus-rate-queries-that-just-work/
    return expr_aggr_func(
        metric=metric,
        aggr_op="count",
        func="rate",
        label_selectors=label_selectors,
        range_selector="$__rate_interval",
        by_labels=by_labels,
        instance_selector=instance_selector,
    )


def expr_simple(
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
) -> Expr:
    """
    Query an instant vector of a metric.

    Example:

    tikv_grpc_msg_duration_seconds_count
    {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
    """
    expr = Expr(metric=metric)
    expr.function("", label_selectors=label_selectors)
    expr.instance_selector = tuple(instance_selector)
    return expr


def expr_operator(
    lhs: Union[Expr, OpExpr, str], operator: str, rhs: Union[Expr, OpExpr, str]
) -> OpExpr:
    return OpExpr(lhs, operator, rhs)


def expr_histogram_quantile(
    quantile: Union[float, str],
    metrics: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = [],
    is_optional_quantile: bool = False,
    use_shared_pool: bool = False,
) -> Expr:
    """
    Query a quantile of a histogram metric.

    Example:

    histogram_quantile(0.99, sum(rate(
        tikv_grpc_msg_duration_seconds_bucket
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance",type!="kv_gc"}
        [$__rate_interval]
    )) by (le))
    """
    # sum(rate(metrics_bucket{label_selectors}[$__rate_interval])) by (le)
    assert not metrics.endswith(
        "_bucket"
    ), f"'{metrics}' should not specify '_bucket' suffix manually"
    by_labels = list(filter(lambda label: label != "le", by_labels))
    sum_rate_of_buckets = expr_sum_rate(
        metrics + "_bucket",
        instance_selector=instance_selector,
        label_selectors=label_selectors,
        by_labels=by_labels + ["le"],
        use_shared_pool=use_shared_pool,
    )
    # histogram_quantile({quantile}, {sum_rate_of_buckets})
    # Keep string quantiles as-is so "0.80" / "1.00" match legacy PromQL.
    quantile_str = quantile if isinstance(quantile, str) else f"{quantile}"
    if is_optional_quantile:
        quantile_str = OPTIONAL_QUANTILE_INPUT
    expr = expr_aggr(
        metric=sum_rate_of_buckets,
        aggr_op="histogram_quantile",
        aggr_param=quantile_str,
        instance_selector=(),
        label_selectors=[],
        by_labels=[],
    ).extra(
        # Do not attach default label selector again.
        default_label_selectors=[],
    )
    # Do not set use_shared_pool for the outer expression. It only needs to be set to the innermost layer.
    return expr


def expr_topk(
    k: int,
    metrics: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
) -> Expr:
    """
    Query topk of a metric.

    Example:

    topk(20, tikv_thread_voluntary_context_switches)
    """
    # topk({k}, {metric}) — outer clears cluster/instance defaults (metric may already be an Expr).
    expr = expr_aggr(
        metric=metrics,
        aggr_op="topk",
        aggr_param=f"{k}",
        instance_selector=(),
        label_selectors=[],
        by_labels=[],
    ).extra(
        # Do not attach default label selector again.
        default_label_selectors=[]
    )
    return expr


def expr_histogram_avg(
    metrics: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = ["instance"],
) -> OpExpr:
    """
    Query the avg of a histogram metric.

    Example:

    sum(rate(
        tikv_grpc_msg_duration_seconds_sum
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance"}
        [$__rate_interval]
    )) / sum(rate(
        tikv_grpc_msg_duration_seconds_count
        {k8s_cluster="$k8s_cluster",tidb_cluster="$tidb_cluster",instance=~"$instance"}
        [$__rate_interval]
    ))
    """
    for suffix in ["_bucket", "_count", "_sum"]:
        assert not metrics.endswith(
            suffix
        ), f"'{metrics}' should not specify '{suffix}' suffix manually"

    return OpExpr(
        expr_sum_rate(
            metrics + "_sum",
            instance_selector=instance_selector,
            label_selectors=label_selectors,
            by_labels=by_labels,
        ),
        "/",
        expr_sum_rate(
            metrics + "_count",
            instance_selector=instance_selector,
            label_selectors=label_selectors,
            by_labels=by_labels,
        ),
    )


def target(
    expr: Union[Expr, OpExpr, str],
    legend_format: Optional[str] = None,
    hide=False,
    data_source=DATASOURCE,
    interval_factor=2,  # Align with legacy TiFlash jsonnet helpers
    # It indicates whether to add additional groupby label to the groupby and legend.
    # If the expr is already groupby by instance, no need to set it to True.
    # Additional groupby is used to support optional by instance. By default, it doesn't
    # take effect. When the variable $additional_groupby of dashboard is set to `instance`.
    # The expr would be groupby by `instance`.
    additional_groupby=False,
) -> Target:
    if isinstance(expr, Expr):
        if legend_format is None and expr.by_labels:
            legend_format = "-".join(
                map(lambda x: "{{" + f"{x}" + "}}", expr.by_labels)
            )
        if additional_groupby:
            expr.append_by_labels(ADDITIONAL_GROUPBY)
            if legend_format is None:
                legend_format = ""
            legend_format += " {{" + ADDITIONAL_GROUPBY + "}}"
    elif isinstance(expr, OpExpr):
        assert legend_format is not None, "legend_format must be specified"
        if additional_groupby:
            expr.append_by_labels(ADDITIONAL_GROUPBY)
            legend_format += " {{" + ADDITIONAL_GROUPBY + "}}"

    return Target(
        expr=f"{expr}",
        hide=hide,
        legendFormat=legend_format,
        intervalFactor=interval_factor,
        datasource=data_source,
    )


def template(
    name,
    type,
    query,
    data_source,
    hide,
    regex=None,
    multi=False,
    include_all=False,
    all_value=None,
    label=None,
    refresh=2,
) -> Template:
    return Template(
        dataSource=data_source,
        hide=hide,
        label=name if label is None else label,
        multi=multi,
        name=name,
        query=query,
        refresh=refresh,
        sort=1,
        type=type,
        useTags=False,
        regex=regex,
        includeAll=include_all,
        allValue=all_value,
    )


class Layout:
    # Rows are always 24 "units" wide.
    ROW_WIDTH = 24
    PANEL_HEIGHT = 8
    row_panel: RowPanel
    current_row_y_pos: int
    current_row_x_pos: int

    def __init__(self, title, collapsed=True, repeat: Optional[str] = None) -> None:
        extraJson = None
        if repeat:
            extraJson = {"repeat": repeat}
            title = f"{title} - ${repeat}"
        self.current_row_y_pos = 0
        self.current_row_x_pos = 0
        self.row_panel = RowPanel(
            title=title,
            gridPos=GridPos(h=self.PANEL_HEIGHT, w=self.ROW_WIDTH, x=0, y=0),
            collapsed=collapsed,
            extraJson=extraJson,
        )

    def row(
        self,
        panels: list[Panel],
        width: int = ROW_WIDTH,
        height: Optional[int] = None,
        widths: Optional[list[int]] = None,
    ):
        """Start a new band and pack panels left-to-right.

        - width: total width budget for this band (default 24)
        - height: band height (default PANEL_HEIGHT)
        - widths: optional per-panel widths; when omitted, split evenly
        """
        count = len(panels)
        if count == 0:
            return panels
        h = self.PANEL_HEIGHT if height is None else height
        if widths is not None:
            assert len(widths) == count
            ws = list(widths)
        else:
            base = width // count
            ws = [base] * count
            ws[-1] += width - base * count
        x = 0  # each band starts at the left edge
        for panel, w in zip(panels, ws):
            panel.gridPos = GridPos(h=h, w=w, x=x, y=self.current_row_y_pos)
            x += w
        self.row_panel.panels.extend(panels)
        self.current_row_y_pos += h
        # Do not carry x into the next band; authors call row()/half_row() per band.
        self.current_row_x_pos = 0

    def half_row(self, panels: list[Panel], height: Optional[int] = None):
        """Place panels in the left half (width 12), leaving the right half empty when alone."""
        self.row(panels, self.ROW_WIDTH // 2, height=height)


def timeseries_panel(
    title,
    targets,
    legend_calcs=["max", "last"],
    unit="s",
    draw_style="line",
    line_width=1,
    fill_opacity=10,
    gradient_mode="opacity",
    tooltip_mode="multi",
    legend_display_mode="table",
    legend_placement="right",
    description=None,
    data_source=DATASOURCE,
) -> TimeSeries:
    return TimeSeries(
        title=title,
        dataSource=data_source,
        description=description,
        targets=targets,
        legendCalcs=legend_calcs,
        drawStyle=draw_style,
        lineWidth=line_width,
        fillOpacity=fill_opacity,
        gradientMode=gradient_mode,
        unit=unit,
        tooltipMode=tooltip_mode,
        legendDisplayMode=legend_display_mode,
        legendPlacement=legend_placement,
    )


def yaxis(
    format: str,
    log_base: int = 1,
    min=None,
    max=None,
    label: Optional[str] = None,
    decimals: Optional[int] = None,
    show: bool = True,
) -> YAxis:
    # CSE forbids SI byte units in favor of IEC; TiFlash Summary historically uses
    # Grafana "bytes"/"Bps" and we preserve those for clinic compatibility.
    return YAxis(
        format=format,
        logBase=log_base,
        min=min,
        max=max,
        label=label,
        decimals=decimals,
        show=show,
    )


def yaxes(
    left_format: str,
    right_format: Optional[str] = None,
    log_base: int = 1,
    left_min=None,
    left_max=None,
    left_label: Optional[str] = None,
    left_decimals: Optional[int] = None,
    left_log_base: Optional[int] = None,
    left_show: bool = True,
    right_min=None,
    right_max=None,
    right_label: Optional[str] = None,
    right_decimals: Optional[int] = None,
    right_log_base: Optional[int] = None,
    right_show: bool = False,
) -> YAxes:
    """Build Y axes. Right axis is hidden by default; pass right_show=True for dual-axis panels."""
    left = yaxis(
        left_format,
        log_base=log_base if left_log_base is None else left_log_base,
        min=left_min,
        max=left_max,
        label=left_label,
        decimals=left_decimals,
        show=left_show,
    )
    # Always set right so grafanalib's default (show=True) never leaks into single-axis panels.
    right = yaxis(
        right_format if right_format is not None else UNITS.SHORT,
        log_base=log_base if right_log_base is None else right_log_base,
        min=right_min,
        max=right_max,
        label=right_label,
        decimals=right_decimals,
        show=right_show,
    )
    return YAxes(left=left, right=right)


def graph_legend(
    avg=False,
    current=True,
    max=True,
    min=False,
    show=True,
    total=False,
    align_as_table=True,
    hide_empty=True,
    hide_zero=True,
    right_side=True,
    side_width=None,
    sort_desc=True,
) -> Legend:
    sort = "max" if max else "current"
    return Legend(
        avg=avg,
        current=current,
        max=max,
        min=min,
        show=show,
        total=total,
        alignAsTable=align_as_table,
        hideEmpty=hide_empty,
        hideZero=hide_zero,
        rightSide=right_side,
        sideWidth=side_width,
        sort=sort,
        sortDesc=sort_desc,
    )


def graph_panel(
    title: str,
    targets: list[Target],
    description=None,
    yaxes=yaxes(left_format=UNITS.NONE_FORMAT),
    legend=None,
    tooltip=Tooltip(shared=True, valueType="individual"),
    lines=True,
    line_width=1,
    fill=0,
    fill_gradient=0,
    stack=False,
    thresholds: list[GraphThreshold] = [],
    series_overrides: list = [],
    data_source=DATASOURCE,
    null_point_mode=NULL_AS_ZERO,
    tooltip_sort: int = 0,
    decimals: Optional[int] = None,
    points: bool = False,
    pointradius: int = 5,
) -> Panel:
    # Internal extraJson patches grafanalib gaps (fillGradient / tooltip.sort / decimals).
    extraJson: dict = {}
    if fill_gradient != 0:
        # fillGradient is only valid when fill is 1.
        if fill == 0:
            fill = 1
        # fillGradient is not set correctly in grafanalib(0.7.0), so we need to
        # set it manually.
        # TODO: remove it when grafanalib fix this.
        extraJson["fillGradient"] = 1

    if tooltip_sort:
        extraJson["tooltip"] = {
            "shared": True,
            "sort": tooltip_sort,
            "value_type": getattr(tooltip, "valueType", "individual"),
        }
    for target in targets:
        # Make sure target is in time_series format.
        if getattr(target, "format", None) in (None, TIME_SERIES_TARGET_FORMAT):
            target.format = TIME_SERIES_TARGET_FORMAT

    if decimals is not None:
        extraJson["decimals"] = decimals

    return Graph(
        title=title,
        dataSource=data_source,
        description=description,
        targets=targets,
        yAxes=yaxes,
        legend=legend if legend else graph_legend(),
        lines=lines,
        bars=not lines,
        lineWidth=line_width,
        fill=fill,
        fillGradient=fill_gradient,
        stack=stack,
        nullPointMode=null_point_mode,
        thresholds=thresholds,
        tooltip=tooltip,
        seriesOverrides=series_overrides,
        # Do not specify max max data points, let Grafana decide.
        maxDataPoints=None,
        points=points,
        pointRadius=pointradius,
        extraJson=extraJson or None,
    )


def series_override(
    alias: str,
    bars: bool = False,
    lines: bool = True,
    yaxis: int = 1,
    fill: int = 1,
    zindex: int = 0,
    dashes: Optional[bool] = None,
    dash_length: Optional[int] = None,
    space_length: Optional[int] = None,
    transform_negative_y: bool = False,
) -> SeriesOverride:
    class SeriesOverridePatch(SeriesOverride):
        dashes_override: Optional[bool]
        dash_length_override: Optional[int]
        space_length_override: Optional[int]
        transform_negative_y: bool

        def __init__(self, *args, **kwargs) -> None:
            self.dashes_override = kwargs["dashes"]
            if self.dashes_override is None:
                del kwargs["dashes"]
            self.dash_length_override = kwargs["dashLength"]
            if self.dash_length_override is None:
                del kwargs["dashLength"]
            self.space_length_override = kwargs["spaceLength"]
            if self.space_length_override is None:
                del kwargs["spaceLength"]
            self.transform_negative_y = kwargs["transform_negative_y"]
            del kwargs["transform_negative_y"]
            super().__init__(*args, **kwargs)

        def to_json_data(self):
            data = super().to_json_data()
            # The default 'null' color makes it transparent, remove it.
            del data["color"]
            # The default 'null' makes it a transparent line, remove it.
            if self.dashes_override is None:
                del data["dashes"]
            if self.dash_length_override is None:
                del data["dashLength"]
            if self.space_length_override is None:
                del data["spaceLength"]
            # Add missing transform.
            if self.transform_negative_y:
                data["transform"] = "negative-Y"
            return data

    return SeriesOverridePatch(
        alias=alias,
        bars=bars,
        lines=lines,
        yaxis=yaxis,
        fill=fill,
        zindex=zindex,
        dashes=dashes,
        dashLength=dash_length,
        spaceLength=space_length,
        transform_negative_y=transform_negative_y,
    )


def heatmap_color() -> HeatmapColor:
    return HeatmapColor(
        cardColor="#b4ff00",
        colorScale="sqrt",
        colorScheme="interpolateSpectral",
        exponent=0.5,
        mode="spectrum",
        max=None,
        min=None,
    )


def heatmap_panel(
    title: str,
    metric: str,
    description=None,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    yaxis=yaxis(UNITS.NO_FORMAT),
    tooltip=Tooltip(shared=True, valueType="individual"),
    color=heatmap_color(),
    decimals=1,
    data_source=DATASOURCE,
    # TiFlash Summary defaults to sum(delta); pass func="increase" for CSE-style.
    func: str = "delta",
) -> Panel:
    assert metric.endswith(
        "_bucket"
    ), f"'{metric}' should be a histogram metric with '_bucket' suffix"
    expr_fn = expr_sum_increase if func == "increase" else expr_sum_delta
    t = target(
        expr=expr_fn(
            metric,
            instance_selector=instance_selector,
            label_selectors=label_selectors,
            by_labels=["le"],
        ),
    )
    # Make sure targets are in heatmap format.
    t.format = "heatmap"
    # Heatmap target legendFormat should be "{{le}}"
    t.legendFormat = "{{le}}"
    # Overrides yaxis decimal places.
    yaxis.decimals = decimals
    return Heatmap(
        title=title,
        dataSource=data_source,
        description=description,
        targets=[t],
        yAxis=yaxis,
        color=color,
        dataFormat="tsbuckets",
        yBucketBound="upper",
        tooltip=tooltip,
        extraJson={"tooltip": {"showHistogram": True, "show": True}},
        hideZeroBuckets=True,
        # Limit data points, because too many data points slows browser when
        # the resolution is too high.
        # See: https://grafana.com/blog/2020/06/23/how-to-visualize-prometheus-histograms-in-grafana/
        maxDataPoints=512,
        # Fix grafana heatmap migration panic if options is null.
        # See: https://github.com/grafana/grafana/blob/v9.5.14/public/app/plugins/panel/heatmap/migrations.ts#L17
        options={},
    )


def stat_panel(
    title: str,
    targets: list[Target],
    description=None,
    format=UNITS.NONE_FORMAT,
    graph_mode="none",
    decimals: Optional[int] = None,
    mappings: Optional[StatValueMappings] = None,
    text_mode: str = "auto",
    data_source=DATASOURCE,
) -> Panel:
    for target in targets:
        # Make sure target is in time_series format.
        target.format = TIME_SERIES_TARGET_FORMAT
    return Stat(
        title=title,
        dataSource=data_source,
        description=description,
        targets=targets,
        format=format,
        graphMode=graph_mode,
        reduceCalc="lastNotNull",
        decimals=decimals,
        mappings=mappings,
        textMode=text_mode,
    )


def graph_panel_histogram_quantiles(
    title: str,
    description: str,
    yaxes: YAxes,
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    by_labels: list[str] = [],
    hide_p9999=False,
    hide_avg=False,
    hide_count=False,
    additional_groupby=True,
    legend_label_prefix="",
) -> Panel:
    """
    Return a graph panel that shows histogram quantiles of a metric.

    Targets:
        - 99.99% quantile
        - 99% quantile
        - avg
        - count
    """

    def legend(prefix, labels):
        if not labels:
            return prefix
        else:
            return "-".join(
                [prefix] + [f"{legend_label_prefix}{{{{{lb}}}}}" for lb in labels]
            )

    return graph_panel(
        title=title,
        description=description,
        targets=[
            target(
                expr=expr_histogram_quantile(
                    0.9999,
                    f"{metric}",
                    instance_selector=instance_selector,
                    label_selectors=label_selectors,
                    by_labels=by_labels,
                ),
                legend_format=legend("99.99%", by_labels),
                hide=hide_p9999,
                additional_groupby=additional_groupby,
            ),
            target(
                expr=expr_histogram_quantile(
                    0.99,
                    f"{metric}",
                    instance_selector=instance_selector,
                    label_selectors=label_selectors,
                    by_labels=by_labels,
                ),
                legend_format=legend("99%", by_labels),
                additional_groupby=additional_groupby,
            ),
            target(
                expr=expr_histogram_avg(
                    metric,
                    instance_selector=instance_selector,
                    label_selectors=label_selectors,
                    by_labels=by_labels,
                ),
                legend_format=legend("avg", by_labels),
                hide=hide_avg,
                additional_groupby=additional_groupby,
            ),
            target(
                expr=expr_sum_rate(
                    f"{metric}_count",
                    instance_selector=instance_selector,
                    label_selectors=label_selectors,
                    by_labels=by_labels,
                ),
                legend_format=legend("count", by_labels),
                hide=hide_count,
                additional_groupby=additional_groupby,
            ),
        ],
        yaxes=yaxes,
        series_overrides=[
            series_override(
                # use regex because the real alias is "count ${additional_groupby}"
                alias="/^count/",
                fill=2,
                yaxis=2,
                zindex=-3,
                dashes=True,
                dash_length=1,
                space_length=1,
                transform_negative_y=True,
            ),
            series_override(
                # use regex because the real alias is "avg ${additional_groupby}"
                alias="/^avg/",
                fill=7,
            ),
        ],
    )


def heatmap_panel_graph_panel_histogram_quantile_pairs(
    heatmap_title: str,
    heatmap_description: str,
    graph_title: str,
    graph_description: str,
    yaxis_format: str,
    metric: str,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors=[],
    graph_by_labels=[],
    graph_hides: list[str] = ["count"],
) -> list[Panel]:
    hide_count = False
    hide_avg = False
    for hide in graph_hides:
        if hide == "count":
            hide_count = True
        elif hide == "avg":
            hide_avg = True

    return [
        heatmap_panel(
            title=heatmap_title,
            description=heatmap_description,
            yaxis=yaxis(format=yaxis_format),
            metric=f"{metric}_bucket",
            instance_selector=instance_selector,
            label_selectors=label_selectors,
        ),
        graph_panel_histogram_quantiles(
            title=graph_title,
            description=graph_description,
            metric=f"{metric}",
            yaxes=yaxes(left_format=yaxis_format),
            instance_selector=instance_selector,
            label_selectors=label_selectors,
            by_labels=graph_by_labels,
            hide_count=hide_count,
            hide_avg=hide_avg,
        ),
    ]


# ---------------------------------------------------------------------------
# TiFlash Summary L3 helpers (ported from tiflashnet/common.libsonnet)
# ---------------------------------------------------------------------------

S3_STYLE_QUANTILES = [
    {"q": "1.00", "name": "max", "hide": True, "max_hack": True},
    {"q": "0.9999", "name": "9999"},
    {"q": "0.999", "name": "999", "hide": True},
    {"q": "0.99", "name": "99"},
    {"q": "0.80", "name": "80", "hide": True},
    {"name": "avg", "hide": True, "avg": True},
]


def _strip_bucket(metric: str) -> str:
    return metric[: -len("_bucket")] if metric.endswith("_bucket") else metric


def by_legend(by_labels: list[str]) -> str:
    return "-".join("{{" + l + "}}" for l in by_labels)


def tiflash_override(
    alias: str,
    yaxis: Optional[int] = None,
    color: Optional[str] = None,
    linewidth: Optional[int] = None,
    fill: Optional[int] = None,
    hide_tooltip: Optional[bool] = None,
    legend: Optional[bool] = None,
    null_point_mode: Optional[str] = None,
    dashes: Optional[bool] = None,
    zindex: Optional[int] = None,
) -> dict:
    """Series override as plain dict (grafanalib SeriesOverride is limited)."""
    data: dict = {"alias": alias}
    if yaxis is not None:
        data["yaxis"] = yaxis
    if color is not None:
        data["color"] = color
    if linewidth is not None:
        data["linewidth"] = linewidth
    if fill is not None:
        data["fill"] = fill
    if hide_tooltip is not None:
        data["hideTooltip"] = hide_tooltip
    if legend is not None:
        data["legend"] = legend
    if null_point_mode is not None:
        data["nullPointMode"] = null_point_mode
    if dashes is not None:
        data["dashes"] = dashes
    if zindex is not None:
        data["zindex"] = zindex
    return data


def duration_panel(
    title: str,
    metric: str,
    description=None,
    by_labels: list[str] = [],
    legend: str = "%s {{$additional_groupby}}",
    unit: str = UNITS.SECONDS,
    y_right: str = UNITS.SHORT,
    quantiles=None,
    show_avg: Optional[bool] = None,
    extra_targets: list[Target] = [],
    series_overrides: list = [],
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    extra_label_selectors: list[str] = [],
) -> Panel:
    """Duration histogram panel with S3-style quantile visibility defaults."""
    metric_base = _strip_bucket(metric)
    qs = [dict(q) for q in (quantiles if quantiles is not None else S3_STYLE_QUANTILES)]
    if show_avg is not None:
        for q in qs:
            if q.get("avg"):
                q["hide"] = not show_avg

    targets: list[Target] = []
    for q in qs:
        hide = bool(q.get("hide"))
        name = q["name"]
        if q.get("avg"):
            expr: Union[Expr, OpExpr, str] = expr_histogram_avg(
                metric_base,
                instance_selector=instance_selector,
                label_selectors=extra_label_selectors,
                by_labels=by_labels + [ADDITIONAL_GROUPBY],
            )
        else:
            # Match jsonnet by (le, <by...>, $additional_groupby)
            bucket_rate = expr_sum_rate(
                metric_base + "_bucket",
                instance_selector=instance_selector,
                label_selectors=extra_label_selectors,
                by_labels=["le"] + by_labels + [ADDITIONAL_GROUPBY],
            )
            q_str = str(q["q"])
            if q.get("max_hack"):
                # maxHack: histogram_quantile(q, sum(round(1e9*rate(...)))/1e9)
                # Keep as raw PromQL for exact parity with jsonnet.
                sel = (
                    list(CLUSTER_LABEL_SELECTORS)
                    + list(instance_selector)
                    + list(extra_label_selectors)
                )
                sel_str = ",".join(sel)
                by_clause = ", ".join(["le"] + by_labels + [ADDITIONAL_GROUPBY])
                expr = (
                    f"histogram_quantile({q_str}, "
                    f"sum(round(1000000000*rate({metric_base}_bucket{{{sel_str}}}"
                    f"[$__rate_interval]))) by ({by_clause}) / 1000000000)"
                )
            else:
                expr = expr_aggr(
                    metric=bucket_rate,
                    aggr_op="histogram_quantile",
                    aggr_param=q_str,
                    instance_selector=(),
                    by_labels=[],
                ).extra(default_label_selectors=[])
        targets.append(target(expr=expr, legend_format=legend % name, hide=hide))
    targets.extend(extra_targets)
    return graph_panel(
        title=title,
        description=description,
        targets=targets,
        yaxes=yaxes(
            left_format=unit,
            right_format=y_right,
            right_show=bool(series_overrides),
        ),
        legend=graph_legend(max=True, current=True, sort_desc=True),
        fill=1,
        fill_gradient=0,
        series_overrides=series_overrides,
        tooltip_sort=2,
        null_point_mode=NULL_AS_ZERO,
    )


def ops_panel(
    title: str,
    metric: str,
    by_labels: list[str] = [],
    legend: Optional[str] = None,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    description=None,
    y_left: str = "ops",
    fill: int = 0,
) -> Panel:
    if legend is None:
        legend = "value" if not by_labels else by_legend(by_labels)
    t = target(
        expr=expr_sum_rate(
            metric,
            instance_selector=instance_selector,
            label_selectors=label_selectors,
            by_labels=by_labels,
        ),
        legend_format=legend,
    )
    return graph_panel(
        title=title,
        description=description,
        targets=[t],
        yaxes=yaxes(left_format=y_left),
        fill=fill,
        fill_gradient=0,
        tooltip_sort=2,
        null_point_mode=NULL_AS_ZERO,
    )


def tiflash_heatmap_panel(
    title: str,
    metric: str,
    description=None,
    y_format: str = UNITS.SECONDS,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    func: str = "delta",
) -> Panel:
    return heatmap_panel(
        title=title,
        metric=metric,
        description=description,
        instance_selector=instance_selector,
        label_selectors=label_selectors,
        yaxis=yaxis(format=y_format),
        func=func,
    )


def cpu_with_limit_panel(
    title: str,
    name_regex: str,
    description=None,
    legend: str = "{{instance}}",
    metric: str = "tiflash_proxy_thread_cpu_seconds_total",
    hide_limit: bool = False,
    instance_selector: Sequence[str] = PROXY_LABEL_SELECTORS,
) -> Panel:
    """Thread CPU + optional Limit count line (proxy instance + role selectors)."""
    targets = [
        target(
            expr=expr_sum_rate(
                metric,
                instance_selector=instance_selector,
                label_selectors=[f'name=~"{name_regex}"'],
                by_labels=["instance"],
            ),
            legend_format=legend,
        )
    ]
    overrides: list = []
    if not hide_limit:
        targets.append(
            target(
                expr=expr_aggr(
                    metric,
                    "count",
                    instance_selector=instance_selector,
                    label_selectors=[f'name=~"{name_regex}"'],
                    by_labels=["instance"],
                ),
                legend_format="Limit",
            )
        )
        overrides.append(
            tiflash_override(
                "Limit",
                color="#F2495C",
                hide_tooltip=True,
                legend=False,
                linewidth=2,
                null_point_mode="connected",
            )
        )
    return graph_panel(
        title=title,
        description=description,
        targets=targets,
        yaxes=yaxes(left_format=UNITS.PERCENT_UNIT, right_format=UNITS.SHORT),
        fill=0,
        fill_gradient=0,
        series_overrides=overrides,
        null_point_mode="null",
    )


def ops_hit_ratio_panel(
    title: str,
    metric: str,
    ratios: list[dict],
    by_labels: list[str] = [],
    legend: Optional[str] = None,
    instance_selector: Sequence[str] = CPP_LABEL_SELECTORS,
    label_selectors: list[str] = [],
    description=None,
    y_left: str = "ops",
    fill: int = 0,
) -> Panel:
    ops_legend = (
        legend
        if legend is not None
        else ("ops" if not by_labels else by_legend(by_labels))
    )
    ops_t = target(
        expr=expr_sum_rate(
            metric,
            instance_selector=instance_selector,
            label_selectors=label_selectors,
            by_labels=by_labels,
        ),
        legend_format=ops_legend,
    )
    ratio_targets = []
    overrides = []
    for r in ratios:
        r_metric = r.get("metric", metric)
        r_by = r.get("by", [])
        hit = r.get("hit_labels", r.get("hitLabels", []))
        total = r.get("total_labels", r.get("totalLabels", []))
        if isinstance(hit, str):
            hit = [hit] if hit else []
        if isinstance(total, str):
            total = [total] if total else []
        hit_expr = expr_sum_rate(
            r_metric,
            instance_selector=instance_selector,
            label_selectors=hit,
            by_labels=r_by,
        )
        tot_expr = expr_sum_rate(
            r_metric,
            instance_selector=instance_selector,
            label_selectors=total,
            by_labels=r_by,
        )
        ratio_targets.append(
            target(
                expr=expr_operator(hit_expr, "/", tot_expr),
                legend_format=r["legend"],
                hide=bool(r.get("hide")),
            )
        )
        overrides.append(
            tiflash_override(
                r.get("override_alias", r.get("overrideAlias", r["legend"])),
                yaxis=2,
            )
        )
    return graph_panel(
        title=title,
        description=description,
        targets=[ops_t] + ratio_targets,
        yaxes=yaxes(
            left_format=y_left,
            right_format=UNITS.PERCENT_UNIT,
            right_show=True,
        ),
        fill=fill,
        fill_gradient=0,
        series_overrides=overrides,
        null_point_mode=NULL_AS_ZERO,
    )


def make_heatmap(
    title: str,
    targets: list[Target],
    description=None,
    y_format: str = UNITS.SECONDS,
    log_base: int = 1,
    hide_zero_buckets: bool = True,
    max_data_points: int = 512,
    data_source=DATASOURCE,
) -> Panel:
    """Build a legacy heatmap panel from pre-built targets (PromQL preserved)."""
    for t in targets:
        t.format = "heatmap"
        if not t.legendFormat:
            t.legendFormat = "{{le}}"
    yax = yaxis(format=y_format, log_base=log_base)
    return Heatmap(
        title=title,
        dataSource=data_source,
        description=description,
        targets=targets,
        yAxis=yax,
        color=heatmap_color(),
        dataFormat="tsbuckets",
        yBucketBound="upper",
        tooltip=Tooltip(shared=True, valueType="individual"),
        extraJson={"tooltip": {"showHistogram": True, "show": True}},
        hideZeroBuckets=hide_zero_buckets,
        maxDataPoints=max_data_points,
        options={},
    )
