
if(USE_THIRDPARTY_LIBRARIES)
  find_package(civetweb-3rdparty CONFIG REQUIRED)
else()
  find_package(civetweb REQUIRED)
endif()

if(ENABLE_COMPRESSION)
  if (NOT ZLIB_FOUND)
    find_package(ZLIB REQUIRED)
  endif()
endif()

add_library(pull
  ${PROMETHEUS_SRC_DIR}/pull/src/basic_auth.cc
  ${PROMETHEUS_SRC_DIR}/pull/src/basic_auth.h
  ${PROMETHEUS_SRC_DIR}/pull/src/endpoint.cc
  ${PROMETHEUS_SRC_DIR}/pull/src/endpoint.h
  ${PROMETHEUS_SRC_DIR}/pull/src/exposer.cc
  ${PROMETHEUS_SRC_DIR}/pull/src/handler.cc
  ${PROMETHEUS_SRC_DIR}/pull/src/handler.h
  ${PROMETHEUS_SRC_DIR}/pull/src/metrics_collector.cc
  ${PROMETHEUS_SRC_DIR}/pull/src/metrics_collector.h

  ${PROMETHEUS_SRC_DIR}/pull/src/detail/base64.h

  $<$<BOOL:${USE_THIRDPARTY_LIBRARIES}>:$<TARGET_OBJECTS:civetweb>>
)

add_library(${PROJECT_NAME}::pull ALIAS pull)

target_link_libraries(pull
  PUBLIC
    ${PROJECT_NAME}::core
  PRIVATE
    Threads::Threads
    ${CIVETWEB_LIBRARIES}
    $<$<AND:$<BOOL:UNIX>,$<NOT:$<BOOL:APPLE>>>:rt>
    $<$<BOOL:${ENABLE_COMPRESSION}>:${ZLIB_LIBRARIES}>
)

target_include_directories(pull
  PUBLIC
    $<BUILD_INTERFACE:${PROMETHEUS_SRC_DIR}/pull/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
  PRIVATE
    ${CIVETWEB_INCLUDE_DIRS}
)

target_compile_definitions(pull
  PRIVATE
    $<$<BOOL:${ENABLE_COMPRESSION}>:HAVE_ZLIB>
)

set_target_properties(pull
  PROPERTIES
    OUTPUT_NAME ${PROJECT_NAME}-pull
    DEFINE_SYMBOL PROMETHEUS_CPP_PULL_EXPORTS
    VERSION "${PROJECT_VERSION}"
    SOVERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}"
)

generate_export_header(pull
  BASE_NAME ${PROJECT_NAME}-pull
  EXPORT_FILE_NAME include/prometheus/detail/pull_export.h
)

# install(
#   TARGETS pull
#   EXPORT ${PROJECT_NAME}-targets
#   RUNTIME DESTINATION  ${CMAKE_INSTALL_BINDIR}
#   LIBRARY DESTINATION  ${CMAKE_INSTALL_LIBDIR}
#   ARCHIVE DESTINATION  ${CMAKE_INSTALL_LIBDIR}
#   INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
# )

# install(
#   DIRECTORY include/ ${CMAKE_CURRENT_BINARY_DIR}/include/
#   DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
# )

if(ENABLE_TESTING)
  add_subdirectory(${PROMETHEUS_SRC_DIR}/pull/tests ${CMAKE_CURRENT_BINARY_DIR}/pull/tests)
endif()
