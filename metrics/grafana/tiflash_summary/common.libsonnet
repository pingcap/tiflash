{
  // Shared helpers for TiFlash Summary dashboard (grafonnet-lib).

  datasource:: '${DS_TEST-CLUSTER}',

  // Common PromQL label matchers used by most TiFlash panels.
  selector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$instance", instance=~"$tiflash_role"',
  proxySelector:: 'k8s_cluster="$k8s_cluster", tidb_cluster="$tidb_cluster", instance=~"$proxy_instance"',

  rowPos:: { x: 0, y: 0, w: 24, h: 1 },

  // gridPos helper; y is filled by Grafana/grafonnet when adding panels under a row.
  pos(w, h):: { x: 0, y: 0, w: w, h: h },
  left(h=7):: { x: 0, y: 0, w: 12, h: h },
  right(h=7):: { x: 12, y: 0, w: 12, h: h },
  full(h=7):: { x: 0, y: 0, w: 24, h: h },
  third(h=8):: { x: 0, y: 0, w: 8, h: h },
}
