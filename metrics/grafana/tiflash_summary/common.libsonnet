{
  // Shared helpers for TiFlash Summary dashboard (grafonnet-lib).

  datasource:: '${DS_TEST-CLUSTER}',

  // Common PromQL label matchers used by most TiFlash panels.
  selector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"',
  proxySelector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance"',

  rowPos:: { x: 0, y: 0, w: 24, h: 1 },

  // gridPos helper. x/y matter for horizontal packing inside a row
  // (e.g. three w=8 panels at x=0/8/16, or pairs at x=0/12).
  pos(w, h, x=0, y=0):: { x: x, y: y, w: w, h: h },
  left(h=7, y=0):: self.pos(12, h, x=0, y=y),
  right(h=7, y=0):: self.pos(12, h, x=12, y=y),
  full(h=7, y=0):: self.pos(24, h, x=0, y=y),
  third(h=8, x=0, y=0):: self.pos(8, h, x=x, y=y),
}
