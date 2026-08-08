local gridW = 24;
local defaultH = 8;

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

{
  // Shared helpers for TiFlash Summary dashboard (grafonnet-lib).

  datasource:: '${DS_TEST-CLUSTER}',

  // Common PromQL label matchers used by most TiFlash panels.
  selector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"',
  proxySelector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance"',

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
}
