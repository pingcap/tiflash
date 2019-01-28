add_executable(mock_server mock_server.cc)

target_include_directories(mock_server PUBLIC ${KVPROTO_INCLUDE_DIR})
target_link_libraries(mock_server kvproto
                                  ${Protobuf_LIBRARY}
                                  gRPC::grpc++_unsecure)

add_executable(mock_client mock_client.cc)
target_include_directories(mock_client PUBLIC ${KVPROTO_INCLUDE_DIR})
target_link_libraries(mock_client kvproto
                                  kv_client
                                  ${Protobuf_LIBRARY}
                                  gRPC::grpc++_unsecure)

