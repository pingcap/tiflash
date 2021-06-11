
if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/tipb/proto/select.proto")
	message (FATAL_ERROR "tipb submodule in contrib/tipb is missing. Try run 'git submodule update --init --recursive'")
endif ()

message(STATUS "Using tipb: ${ClickHouse_SOURCE_DIR}/contrib/tipb/cpp")
