set(kvClient_sources)

list(APPEND kvClient_sources pd/Client.cc)
list(APPEND kvClient_sources tikv/Region.cc)
list(APPEND kvClient_sources tikv/Snapshot.cc)
list(APPEND kvClient_sources tikv/Backoff.cc)
list(APPEND kvClient_sources tikv/Rpc.cc)

set(kvClient_INCLUDE_DIR ${kvClient_SOURCE_DIR}/include)

add_library(kv_client ${kvClient_sources})
target_include_directories(kv_client PUBLIC ${KVPROTO_INCLUDE_DIR} ${kvClient_INCLUDE_DIR})
target_link_libraries(kv_client kvproto ${Poco_Foundation_LIBRARY})

add_subdirectory (test)
