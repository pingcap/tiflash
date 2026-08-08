local gridW = 24;
local defaultH = 8;

local grafana = import 'grafonnet/grafana.libsonnet';
local graphPanel = grafana.graphPanel;
local prometheus = grafana.prometheus;

local normalizeItem(item) =
  if std.type(item) == 'object' && std.objectHas(item, 'panel') then
    item
  else
    { panel: item };

// Apply one horizontal band onto a row starting at y.
// Returns { row, nextY }.
local applyBand(rowObj, band, y) =
  local items = std.map(normalizeItem, band.panels);
  local n = std.length(items);
  local h = band.h;
  local eqW = std.floor(gridW / n);
  local widths = std.map(
    function(it) if std.objectHas(it, 'w') then it.w else eqW,
    items
  );
  // xs[i] = sum(widths[0..i-1]); length n+1, last unused.
  local xs = std.foldl(
    function(acc, w) acc + [acc[std.length(acc) - 1] + w],
    widths,
    [0]
  );
  {
    row: std.foldl(
      function(r, i)
        r.addPanel(
          items[i].panel,
          gridPos={ x: xs[i], y: y, w: widths[i], h: h },
        ),
      std.range(0, n - 1),
      rowObj
    ),
    nextY: y + h,
  };

// Default quantiles for duration panels.
// Only p9999 and p99 are shown by default; others (incl. avg) are hidden.
local s3StyleQuantiles = [
  { q: '1.00', name: 'max', hide: true, maxHack: true },
  { q: '0.9999', name: '9999' },
  { q: '0.999', name: '999', hide: true },
  { q: '0.99', name: '99' },
  { q: '0.80', name: '80', hide: true },
  { name: 'avg', hide: true, avg: true },
];

local byClause(byLabels) =
  std.join(', ', ['le'] + byLabels + ['$additional_groupby']);

local avgByClause(byLabels) =
  std.join(', ', byLabels + ['$additional_groupby']);

local durationExpr(metric, selector, byLabels, q, range, maxHack) =
  local metricSel = metric + '{' + selector + '}';
  local by = byClause(byLabels);
  if maxHack then
    'histogram_quantile(' + q + ', sum(round(1000000000*rate(' + metricSel + '[' + range + ']))) by (' + by + ') / 1000000000)'
  else
    'histogram_quantile(' + q + ', sum(rate(' + metricSel + '[' + range + '])) by (' + by + '))';

// avg = rate(sum) / rate(count); metric is expected to end with `_bucket`.
local avgExpr(metric, selector, byLabels, range) =
  local sumMetric = std.strReplace(metric, '_bucket', '_sum');
  local countMetric = std.strReplace(metric, '_bucket', '_count');
  local by = avgByClause(byLabels);
  '(sum(rate(' + sumMetric + '{' + selector + '}[' + range + '])) by (' + by + ') / sum(rate(' + countMetric + '{' + selector + '}[' + range + '])) by (' + by + '))';

local makeDurationTarget(expr, legend, intervalFactor, hide) =
  if intervalFactor == null then
    if hide then
      prometheus.target(expr, legendFormat=legend, hide=true)
    else
      prometheus.target(expr, legendFormat=legend)
  else if hide then
    prometheus.target(expr, legendFormat=legend, intervalFactor=intervalFactor, hide=true)
  else
    prometheus.target(expr, legendFormat=legend, intervalFactor=intervalFactor);

{
  // Shared helpers for TiFlash Summary dashboard (grafonnet-lib).

  datasource:: '${DS_TEST-CLUSTER}',

  // Common PromQL label matchers used by most TiFlash panels.
  selector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"',
  proxySelector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"',

  gridW:: gridW,
  panelH:: defaultH,
  rowPos:: { x: 0, y: 0, w: gridW, h: 1 },

  // Low-level gridPos helper (prefer band/buildRow for row layouts).
  pos(w, h, x=0, y=0):: { x: x, y: y, w: w, h: h },
  left(h=defaultH, y=0):: self.pos(12, h, x=0, y=y),
  right(h=defaultH, y=0):: self.pos(12, h, x=12, y=y),
  full(h=defaultH, y=0):: self.pos(gridW, h, x=0, y=y),

  // A horizontal band: N panels share the same y and are packed left-to-right.
  // Items are either a panel object, or { panel: p, w: <width> } for custom widths.
  // When widths are omitted, panels are equally divided across gridW (24).
  band(panels, h=defaultH):: {
    panels: panels,
    h: h,
  },

  // Build a row by stacking horizontal bands. Authors list panels per band;
  // x/y/w are computed automatically.
  buildRow(rowObj, bands, startY=0)::
    std.foldl(
      function(acc, band)
        local step = applyBand(acc.row, band, acc.y);
        { row: step.row, y: step.nextY },
      bands,
      { row: rowObj, y: startY }
    ).row,

  // Build prometheus targets for histogram duration panels (xxx_seconds_bucket).
  // legend: format string, "%s" is replaced by quantile/avg name (max/9999/99/avg).
  // by: extra labels besides le and $additional_groupby (e.g. ['type']).
  // Entries with avg=true emit sum/count average (no `le` in by).
  durationQuantileTargets(
    metric,
    selector=self.selector,
    by=[],
    legend='%s {{$additional_groupby}}',
    range='$__rate_interval',
    intervalFactor=2,
    quantiles=s3StyleQuantiles,
  )::
    std.map(
      function(q)
        local isAvg = std.objectHas(q, 'avg') && q.avg;
        makeDurationTarget(
          if isAvg then
            avgExpr(metric, selector, by, range)
          else
            durationExpr(
              metric,
              selector,
              by,
              q.q,
              range,
              std.objectHas(q, 'maxHack') && q.maxHack,
            ),
          legend % q.name,
          intervalFactor,
          std.objectHas(q, 'hide') && q.hide,
        ),
      quantiles
    ),

  // Full graph panel for duration histograms
  // (hidden max/p999/p80/avg + visible p9999/p99).
  // Fixed style: intervalFactor=2, fill=1, legend sorted by max desc.
  // Y-axes: left s/min0, right short.
  durationPanel(
    title,
    metric,
    selector=self.selector,
    by=[],
    legend='%s {{$additional_groupby}}',
    range='$__rate_interval',
    description=null,
  )::
    local targets = self.durationQuantileTargets(
      metric,
      selector=selector,
      by=by,
      legend=legend,
      range=range,
      intervalFactor=2,
    );
    local panel = graphPanel.new(
      title=title,
      datasource=self.datasource,
      description=description,
      fill=1,
      nullPointMode='null as zero',
      legend_alignAsTable=true,
      legend_rightSide=true,
      legend_values=true,
      legend_current=true,
      legend_max=true,
      legend_sort='max',
      legend_sortDesc=true,
    );
    std.foldl(
      function(p, t) p.addTarget(t),
      targets,
      panel
    )
    .resetYaxes()
    .addYaxis(format='s', min='0')
    .addYaxis(format='short'),
}
