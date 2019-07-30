
if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/tipb/cpp/tipb/select.pb.h")
	if (EXISTS "${ClickHouse_SOURCE_DIR}/contrib/tipb/proto/select.proto")
		message (FATAL_ERROR "tipb cpp files in contrib/tipb is missing. Try go to contrib/tipb, and run ./generate_cpp.sh")
	else()
		message (FATAL_ERROR "tipb submodule in contrib/tipb is missing. Try run 'git submodule update --init --recursive', and go to contrib/tipb, and run ./generate_cpp.sh")
	endif()
endif ()

message(STATUS "Using tipb: ${ClickHouse_SOURCE_DIR}/contrib/tipb/cpp")
