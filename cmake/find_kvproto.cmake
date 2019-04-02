# Currently kvproto should always use bundled library.

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/kvproto/cpp/kvproto/errorpb.pb.h")
	if (EXISTS "${ClickHouse_SOURCE_DIR}/contrib/kvproto/proto/errorpb.proto")
		message (FATAL_ERROR "kvproto cpp files in contrib/kvproto is missing. Try go to contrib/kvproto, and run ./generate_cpp.sh")
	else()
		message (FATAL_ERROR "kvproto submodule in contrib/kvproto is missing. Try run 'git submodule update --init --recursive', and go to contrib/kvproto, and run ./generate_cpp.sh")
	endif()
endif ()

message(STATUS "Using kvproto: ${ClickHouse_SOURCE_DIR}/contrib/kvproto/cpp")

set (KVPROTO_FOUND TRUE)
