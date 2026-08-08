local gridW = 24;
local defaultH = 8;

local grafana = import 'grafonnet/grafana.libsonnet';
local graphPanel = grafana.graphPanel;
local heatmapPanel = grafana.heatmapPanel;
local prometheus = grafana.prometheus;
local promql = import 'promql.libsonnet';

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

// durationPanel still accepts metric ending with `_bucket`; strip for promql helpers.
local stripBucket(metric) =
  if std.endsWith(metric, '_bucket') then
    std.substr(metric, 0, std.length(metric) - std.length('_bucket'))
  else
    metric;

// L2: prometheus target factory.
local mkTarget(expr, legend, hide=false, intervalFactor=2) =
  if hide then
    prometheus.target(expr, legendFormat=legend, intervalFactor=intervalFactor, hide=true)
  else
    prometheus.target(expr, legendFormat=legend, intervalFactor=intervalFactor);

// L2: series override object (null fields pruned).
local mkOverride(
  alias,
  yaxis=null,
  color=null,
  linewidth=null,
  fill=null,
  hideTooltip=null,
  legend=null,
  nullPointMode=null,
  dashes=null,
  zindex=null,
) =
  std.prune({
    alias: alias,
    yaxis: yaxis,
    color: color,
    linewidth: linewidth,
    fill: fill,
    hideTooltip: hideTooltip,
    legend: legend,
    nullPointMode: nullPointMode,
    dashes: dashes,
    zindex: zindex,
  });

// L2: graph panel style factory.
// Bundles shared legend table style + dual Y axes; optional visual knobs default to
// grafonnet graphPanel.new defaults so existing L3 callers stay equivalent.
local mkGraph(
  title,
  targets,
  datasource,
  description=null,
  fill=1,
  linewidth=1,
  decimals=null,
  nullPointMode='null as zero',
  points=false,
  pointradius=5,
  stack=false,
  legendSort='max',
  legendSortDesc=true,
  legendCurrent=true,
  legendMax=true,
  legendAvg=false,
  legendHideZero=null,
  legendHideEmpty=null,
  sideWidth=null,
  yLeft='short',
  yRight='short',
  yLeftMin='0',
  yLeftMax=null,
  yLeftDecimals=null,
  yRightMin=null,
  yRightMax=null,
  yRightDecimals=null,
  yRightShow=false,
  seriesOverrides=[],
) =
  local base = graphPanel.new(
    title=title,
    datasource=datasource,
    description=description,
    fill=fill,
    linewidth=linewidth,
    decimals=decimals,
    nullPointMode=nullPointMode,
    points=points,
    pointradius=pointradius,
    stack=stack,
    legend_alignAsTable=true,
    legend_rightSide=true,
    legend_values=true,
    legend_current=legendCurrent,
    legend_max=legendMax,
    legend_avg=legendAvg,
    legend_sort=legendSort,
    legend_sortDesc=legendSortDesc,
    legend_sideWidth=sideWidth,
    legend_hideZero=legendHideZero,
    legend_hideEmpty=legendHideEmpty,
  );
  local withTargets = std.foldl(
    function(p, t) p.addTarget(t),
    targets,
    base
  );
  local withAxes = withTargets
    .resetYaxes()
    .addYaxis(format=yLeft, min=yLeftMin, max=yLeftMax, decimals=yLeftDecimals)
    .addYaxis(format=yRight, min=yRightMin, max=yRightMax, show=yRightShow, decimals=yRightDecimals);
  std.foldl(
    function(p, o) p.addSeriesOverride(o),
    seriesOverrides,
    withAxes
  );

{
  // Shared helpers for TiFlash Summary dashboard (grafonnet-lib).

  datasource:: '${DS_TEST-CLUSTER}',

  // Common PromQL label matchers used by most TiFlash panels.
  selector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"',
  proxySelector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance", instance=~"$tiflash_role"',

  // L1: PromQL expression builders (see promql.libsonnet).
  expr:: promql,

  // L2: target / graph / override factories.
  target(expr, legend, hide=false, intervalFactor=2)::
    mkTarget(expr, legend, hide=hide, intervalFactor=intervalFactor),

  override(
    alias,
    yaxis=null,
    color=null,
    linewidth=null,
    fill=null,
    hideTooltip=null,
    legend=null,
    nullPointMode=null,
    dashes=null,
    zindex=null,
  )::
    mkOverride(
      alias,
      yaxis=yaxis,
      color=color,
      linewidth=linewidth,
      fill=fill,
      hideTooltip=hideTooltip,
      legend=legend,
      nullPointMode=nullPointMode,
      dashes=dashes,
      zindex=zindex,
    ),

  graph(
    title,
    targets,
    description=null,
    fill=1,
    linewidth=1,
    decimals=null,
    nullPointMode='null as zero',
    points=false,
    pointradius=5,
    stack=false,
    legendSort='max',
    legendSortDesc=true,
    legendCurrent=true,
    legendMax=true,
    legendAvg=false,
    legendHideZero=null,
    legendHideEmpty=null,
    sideWidth=null,
    yLeft='short',
    yRight='short',
    yLeftMin='0',
    yLeftMax=null,
    yLeftDecimals=null,
    yRightMin=null,
    yRightMax=null,
    yRightDecimals=null,
    yRightShow=false,
    seriesOverrides=[],
  )::
    mkGraph(
      title,
      targets,
      self.datasource,
      description=description,
      fill=fill,
      linewidth=linewidth,
      decimals=decimals,
      nullPointMode=nullPointMode,
      points=points,
      pointradius=pointradius,
      stack=stack,
      legendSort=legendSort,
      legendSortDesc=legendSortDesc,
      legendCurrent=legendCurrent,
      legendMax=legendMax,
      legendAvg=legendAvg,
      legendHideZero=legendHideZero,
      legendHideEmpty=legendHideEmpty,
      sideWidth=sideWidth,
      yLeft=yLeft,
      yRight=yRight,
      yLeftMin=yLeftMin,
      yLeftMax=yLeftMax,
      yLeftDecimals=yLeftDecimals,
      yRightMin=yRightMin,
      yRightMax=yRightMax,
      yRightDecimals=yRightDecimals,
      yRightShow=yRightShow,
      seriesOverrides=seriesOverrides,
    ),

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
    local metricBase = stripBucket(metric);
    std.map(
      function(q)
        local isAvg = std.objectHas(q, 'avg') && q.avg;
        mkTarget(
          if isAvg then
            promql.histogramAvg(metricBase, selector, by=by, range=range)
          else
            promql.histogramQuantile(
              q.q,
              metricBase,
              selector,
              by=by,
              range=range,
              maxHack=std.objectHas(q, 'maxHack') && q.maxHack,
            ),
          legend % q.name,
          hide=std.objectHas(q, 'hide') && q.hide,
          intervalFactor=intervalFactor,
        ),
      quantiles
    ),

  // Full graph panel for duration histograms
  // (hidden max/p999/p80/avg + visible p9999/p99).
  // Fixed style: intervalFactor=2, fill=1, legend sorted by max desc.
  // Y-axes: left s/min0, right short.
  // Default quantile set for durationPanel (hidden max/p999/p80/avg + visible p9999/p99).
  defaultDurationQuantiles:: s3StyleQuantiles,

  // Full graph panel for duration histograms.
  // metric may end with `_bucket`. Optional: unit, quantiles, showAvg, extraTargets, seriesOverrides.
  // Right axis is shown only when seriesOverrides put series on yaxis 2.
  durationPanel(
    title,
    metric,
    selector=self.selector,
    by=[],
    legend='%s {{$additional_groupby}}',
    range='$__rate_interval',
    description=null,
    unit='s',
    yRight='short',
    quantiles=null,
    showAvg=null,
    extraTargets=[],
    seriesOverrides=[],
  )::
    local baseQs = if quantiles == null then s3StyleQuantiles else quantiles;
    local qs =
      if showAvg == null then
        baseQs
      else
        std.map(
          function(q)
            if std.objectHas(q, 'avg') && q.avg then
              q { hide: !showAvg }
            else
              q,
          baseQs
        );
    self.graph(
      title,
      self.durationQuantileTargets(
        metric,
        selector=selector,
        by=by,
        legend=legend,
        range=range,
        intervalFactor=2,
        quantiles=qs,
      ) + extraTargets,
      description=description,
      fill=1,
      nullPointMode='null as zero',
      legendSort='max',
      legendSortDesc=true,
      yLeft=unit,
      yRight=yRight,
      yLeftMin='0',
      yRightShow=std.length(seriesOverrides) > 0,
      seriesOverrides=seriesOverrides,
    ),

  // L3a: single-metric sum(rate) OPS/QPS panel.
  opsPanel(
    title,
    metric,
    by=[],
    legend=null,
    labels='',
    selector=self.selector,
    description=null,
    yLeft='ops',
    yRight='none',
    fill=0,
    range='$__rate_interval',
  )::
    local leg =
      if legend != null then
        legend
      else if std.length(by) == 0 then
        'value'
      else
        promql.byLegend(by);
    self.graph(
      title,
      [
        self.target(
          promql.sumRate(metric, selector, by=by, labels=labels, range=range),
          leg,
        ),
      ],
      description=description,
      fill=fill,
      nullPointMode='null as zero',
      yLeft=yLeft,
      yRight=yRight,
      // Single-series OPS panels do not use the right axis.
      yRightShow=false,
    ),

  // L3b: histogram heatmap (tsbuckets + spectral).
  // metric should end with `_bucket`. Default aggregation is sum(delta(...)) by (le).
  heatmap(
    title,
    metric,
    yFormat='s',
    labels='',
    by=['le'],
    selector=self.selector,
    description=null,
    range='$__rate_interval',
    func='delta',
  )::
    local expr =
      if func == 'increase' then
        promql.sumIncrease(metric, selector, by=by, labels=labels, range=range)
      else
        promql.sumDelta(metric, selector, by=by, labels=labels, range=range);
    heatmapPanel.new(
      title=title,
      datasource=self.datasource,
      description=description,
      dataFormat='tsbuckets',
      yAxis_format=yFormat,
      hideZeroBuckets=true,
      color_mode='spectrum',
      color_colorScheme='interpolateSpectral',
      legend_show=true,
    )
    .addTarget(
      prometheus.target(
        expr,
        format='heatmap',
        legendFormat='{{le}}',
      )
    ),

  // L3c: thread CPU usage + Limit red line (count of matching series).
  // nameRegex is placed inside PromQL name=~"..."; escape for PromQL strings
  // (e.g. regex \d needs jsonnet '\\\\d').
  // hideLimit=true: only the CPU usage series (no Limit count line).
  cpuWithLimitPanel(
    title,
    nameRegex,
    description=null,
    legend='{{instance}}',
    metric='tiflash_proxy_thread_cpu_seconds_total',
    range='$__rate_interval',
    hideLimit=false,
  )::
    local sel = 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", name=~"' + nameRegex + '", instance=~"$tiflash_role"';
    local usageTarget = self.target(promql.sumRate(metric, sel, by=['instance'], range=range), legend);
    local limitTarget = self.target(promql.count(metric, sel, by=['instance']), 'Limit');
    local limitOverride = self.override(
      'Limit',
      color='#F2495C',
      hideTooltip=true,
      legend=false,
      linewidth=2,
      nullPointMode='connected',
    );
    self.graph(
      title,
      if hideLimit then [usageTarget] else [usageTarget, limitTarget],
      description=description,
      fill=0,
      nullPointMode='null',
      yLeft='percentunit',
      yRight='short',
      seriesOverrides=if hideLimit then [] else [limitOverride],
    ),

  // L3d: OPS series + hit-ratio series on the right (percentunit) axis.
  // ratios: [{ hitLabels, totalLabels, legend, hide?, by?, metric? }, ...]
  opsHitRatioPanel(
    title,
    metric,
    ratios,
    by=[],
    legend=null,
    labels='',
    selector=self.selector,
    description=null,
    yLeft='ops',
    fill=0,
    range='$__rate_interval',
  )::
    local opsLegend =
      if legend != null then
        legend
      else if std.length(by) == 0 then
        'ops'
      else
        promql.byLegend(by);
    local opsTarget = self.target(
      promql.sumRate(metric, selector, by=by, labels=labels, range=range),
      opsLegend,
    );
    local ratioTargets = std.map(
      function(r)
        local rMetric = if std.objectHas(r, 'metric') then r.metric else metric;
        local rBy = if std.objectHas(r, 'by') then r.by else [];
        local hitLabels = if std.objectHas(r, 'hitLabels') then r.hitLabels else '';
        local totalLabels = if std.objectHas(r, 'totalLabels') then r.totalLabels else '';
        local hide = std.objectHas(r, 'hide') && r.hide;
        self.target(
          '(' + promql.sumRate(rMetric, selector, by=rBy, labels=hitLabels, range=range)
          + ' / ' + promql.sumRate(rMetric, selector, by=rBy, labels=totalLabels, range=range) + ')',
          r.legend,
          hide=hide,
        ),
      ratios
    );
    local overrides = std.map(
      function(r)
        local alias =
          if std.objectHas(r, 'overrideAlias') then
            r.overrideAlias
          else
            r.legend;
        self.override(alias, yaxis=2),
      ratios
    );
    self.graph(
      title,
      [opsTarget] + ratioTargets,
      description=description,
      fill=fill,
      nullPointMode='null as zero',
      yLeft=yLeft,
      yRight='percentunit',
      yRightShow=true,
      seriesOverrides=overrides,
    ),
}
