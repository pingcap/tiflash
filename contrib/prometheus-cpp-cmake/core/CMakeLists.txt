
add_library(core
  ${PROMETHEUS_SRC_DIR}/core/src/check_names.cc
  ${PROMETHEUS_SRC_DIR}/core/src/counter.cc
  ${PROMETHEUS_SRC_DIR}/core/src/detail/builder.cc
  ${PROMETHEUS_SRC_DIR}/core/src/detail/ckms_quantiles.cc
  ${PROMETHEUS_SRC_DIR}/core/src/detail/time_window_quantiles.cc
  ${PROMETHEUS_SRC_DIR}/core/src/detail/utils.cc
  ${PROMETHEUS_SRC_DIR}/core/src/family.cc
  ${PROMETHEUS_SRC_DIR}/core/src/gauge.cc
  ${PROMETHEUS_SRC_DIR}/core/src/histogram.cc
  ${PROMETHEUS_SRC_DIR}/core/src/registry.cc
  ${PROMETHEUS_SRC_DIR}/core/src/serializer.cc
  ${PROMETHEUS_SRC_DIR}/core/src/summary.cc
  ${PROMETHEUS_SRC_DIR}/core/src/text_serializer.cc
)

add_library(${PROJECT_NAME}::core ALIAS core)

target_link_libraries(core
  PRIVATE
    Threads::Threads
    $<$<AND:$<BOOL:UNIX>,$<NOT:$<BOOL:APPLE>>>:rt>
)

target_include_directories(core
  PUBLIC
    $<BUILD_INTERFACE:${PROMETHEUS_SRC_DIR}/core/include>
    $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
)

set_target_properties(core
  PROPERTIES
    OUTPUT_NAME ${PROJECT_NAME}-core
    DEFINE_SYMBOL PROMETHEUS_CPP_CORE_EXPORTS
    VERSION "${PROJECT_VERSION}"
    SOVERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}"
)

generate_export_header(core
  BASE_NAME ${PROJECT_NAME}-core
  EXPORT_FILE_NAME include/prometheus/detail/core_export.h
)

# install(
#   TARGETS core
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
  add_subdirectory(${PROMETHEUS_SRC_DIR}/core/tests ${CMAKE_CURRENT_BINARY_DIR}/core/tests)
endif()

if(benchmark_FOUND)
  add_subdirectory(${PROMETHEUS_SRC_DIR}/core/benchmarks ${CMAKE_CURRENT_BINARY_DIR}/core/benchmarks)
endif()
