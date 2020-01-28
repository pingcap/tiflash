# Currently prometheus cpp should always use bundled library.

message(STATUS "Using prometheus: ${ClickHouse_SOURCE_DIR}/contrib/prometheus-cpp")

set (PROMETHEUS_CPP_FOUND TRUE)
