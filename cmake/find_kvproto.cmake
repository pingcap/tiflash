# Currently kvproto should always use bundled library.

if (NOT EXISTS "${TiFlash_SOURCE_DIR}/contrib/kvproto/proto/errorpb.proto")
	message (FATAL_ERROR "kvproto submodule in contrib/kvproto is missing. Try run 'git submodule update --init --recursive'")
endif ()

message(STATUS "Using kvproto: ${TiFlash_SOURCE_DIR}/contrib/kvproto/cpp")

set (KVPROTO_FOUND TRUE)
