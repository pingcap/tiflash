// PromQL expression builders for TiFlash Summary (CSE common.py-inspired).
// Functions return PromQL strings; callers pass selector explicitly.

local joinSelector(selector, labels) =
  if labels == '' || labels == null then
    selector
  else
    selector + ', ' + labels;

local byClause(byLabels, extra=[]) =
  local labels = byLabels + extra;
  if std.length(labels) == 0 then
    ''
  else
    ' by (' + std.join(', ', labels) + ')';

local metricSel(metric, selector) =
  metric + '{' + selector + '}';

{
  // Build legend like "{{type}}-{{instance}}" from by labels.
  byLegend(byLabels)::
    std.join('-', std.map(function(l) '{{' + l + '}}', byLabels)),

  // sum(metric{sel}) by (...)
  sum(metric, selector, by=[], labels='')::
    'sum(' + metricSel(metric, joinSelector(selector, labels)) + ')' + byClause(by),

  // max(metric{sel}) by (...)
  max(metric, selector, by=[], labels='')::
    'max(' + metricSel(metric, joinSelector(selector, labels)) + ')' + byClause(by),

  // count(metric{sel}) by (...)
  count(metric, selector, by=[], labels='')::
    'count(' + metricSel(metric, joinSelector(selector, labels)) + ')' + byClause(by),

  // sum(rate(metric{sel}[range])) by (...)
  sumRate(metric, selector, by=[], labels='', range='$__rate_interval')::
    'sum(rate(' + metricSel(metric, joinSelector(selector, labels)) + '[' + range + ']))' + byClause(by),

  // sum(irate(metric{sel}[range])) by (...)
  sumIrate(metric, selector, by=[], labels='', range='$__rate_interval')::
    'sum(irate(' + metricSel(metric, joinSelector(selector, labels)) + '[' + range + ']))' + byClause(by),

  // sum(delta(metric{sel}[range])) by (...)
  sumDelta(metric, selector, by=[], labels='', range='$__rate_interval')::
    'sum(delta(' + metricSel(metric, joinSelector(selector, labels)) + '[' + range + ']))' + byClause(by),

  // sum(increase(metric{sel}[range])) by (...)
  sumIncrease(metric, selector, by=[], labels='', range='$__rate_interval')::
    'sum(increase(' + metricSel(metric, joinSelector(selector, labels)) + '[' + range + ']))' + byClause(by),

  // histogram_quantile(q, sum(rate(metric_bucket{sel}[range])) by (le, ...))
  // metricBase must NOT end with `_bucket`.
  // by order: le, <by...>, $additional_groupby
  histogramQuantile(q, metricBase, selector, by=[], labels='', range='$__rate_interval', maxHack=false)::
    local bucket = metricBase + '_bucket';
    local byLe = ' by (' + std.join(', ', ['le'] + by + ['$additional_groupby']) + ')';
    local sel = joinSelector(selector, labels);
    if maxHack then
      'histogram_quantile(' + q + ', sum(round(1000000000*rate(' + metricSel(bucket, sel) + '[' + range + '])))' + byLe + ' / 1000000000)'
    else
      'histogram_quantile(' + q + ', sum(rate(' + metricSel(bucket, sel) + '[' + range + ']))' + byLe + ')',

  // avg = rate(sum) / rate(count); metricBase must NOT end with `_bucket`/`_sum`/`_count`.
  // by order: <by...>, $additional_groupby (no le)
  histogramAvg(metricBase, selector, by=[], labels='', range='$__rate_interval')::
    local sel = joinSelector(selector, labels);
    local byAvg = ' by (' + std.join(', ', by + ['$additional_groupby']) + ')';
    '(sum(rate(' + metricSel(metricBase + '_sum', sel) + '[' + range + ']))' + byAvg
    + ' / sum(rate(' + metricSel(metricBase + '_count', sel) + '[' + range + ']))' + byAvg + ')',
}
