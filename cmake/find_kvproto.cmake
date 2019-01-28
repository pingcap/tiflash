# Currently kvproto should always use bundled library.

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/kvproto/cpp/kvproto/errorpb.pb.h")
   message (FATAL_ERROR "kvproto submodule in contrib/kvproto is missing.")
endif ()

message(STATUS "Using kvproto: ${ClickHouse_SOURCE_DIR}/contrib/kvproto/cpp")

set (KVPROTO_FOUND TRUE)
