
if (NOT EXISTS "${TIFLASH_SOURCE_DIR}/contrib/tipb/proto/select.proto")
	message (FATAL_ERROR "tipb submodule in contrib/tipb is missing. Try run 'git submodule update --init --recursive'")
endif ()

message(STATUS "Using tipb: ${TIFLASH_SOURCE_DIR}/contrib/tipb/cpp")
