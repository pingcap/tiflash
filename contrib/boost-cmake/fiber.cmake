set (BOOST_FIBER_LIBRARY_DIR ${TiFlash_SOURCE_DIR}/contrib/boost/libs/fiber)

if(WIN32 AND NOT CMAKE_CXX_PLATFORM_ID MATCHES "Cygwin")
  set(_default_target windows)
elseif(CMAKE_SYSTEM_NAME STREQUAL Linux)
  set(_default_target linux)
else()
  set(_default_target none)
endif()

set(BOOST_FIBER_NUMA_TARGET_OS "${_default_target}" CACHE STRING "Boost.Fiber target OS (aix, freebsd, hpux, linux, solaris, windows, none)")
set_property(CACHE BOOST_FIBER_NUMA_TARGET_OS PROPERTY STRINGS aix freebsd hpux linux solaris windows none)

unset(_default_target)

message(STATUS "Boost.Fiber: NUMA target OS is ${BOOST_FIBER_NUMA_TARGET_OS}")

# boost_fiber

add_library(boost_fiber
  ${BOOST_FIBER_LIBRARY_DIR}/src/algo/algorithm.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/algo/round_robin.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/algo/shared_work.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/algo/work_stealing.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/barrier.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/condition_variable.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/context.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/fiber.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/future.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/mutex.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/properties.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/recursive_mutex.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/recursive_timed_mutex.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/scheduler.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/timed_mutex.cpp
  ${BOOST_FIBER_LIBRARY_DIR}/src/waker.cpp
)

add_library(Boost::fiber ALIAS boost_fiber)

target_include_directories (boost_fiber BEFORE PUBLIC ${Boost_INCLUDE_DIRS})

target_compile_features(boost_fiber PUBLIC cxx_std_11)

target_compile_definitions(boost_fiber
  PUBLIC BOOST_FIBER_NO_LIB
  PRIVATE BOOST_FIBER_SOURCE BOOST_FIBERS_SOURCE
)

target_link_libraries (boost_fiber boost_context)

target_no_warning(boost_fiber unused-but-set-variable)

if(BUILD_SHARED_LIBS)
  target_compile_definitions(boost_fiber PUBLIC BOOST_FIBER_DYN_LINK BOOST_FIBERS_DYN_LINK)
else()
  target_compile_definitions(boost_fiber PUBLIC BOOST_FIBER_STATIC_LINK)
endif()

# boost_fiber_numa

if(BOOST_FIBER_NUMA_TARGET_OS STREQUAL none)
  set(BOOST_FIBER_NUMA_SOURCES
    ${BOOST_FIBER_LIBRARY_DIR}/src/numa/pin_thread.cpp
    ${BOOST_FIBER_LIBRARY_DIR}/src/numa/topology.cpp
  )
else()
  set(BOOST_FIBER_NUMA_SOURCES
    ${BOOST_FIBER_LIBRARY_DIR}/src/numa/${BOOST_FIBER_NUMA_TARGET_OS}/pin_thread.cpp
    ${BOOST_FIBER_LIBRARY_DIR}/src/numa/${BOOST_FIBER_NUMA_TARGET_OS}/topology.cpp
  )
endif()

add_library(boost_fiber_numa
  ${BOOST_FIBER_NUMA_SOURCES}
  ${BOOST_FIBER_LIBRARY_DIR}/src/numa/algo/work_stealing.cpp
)

target_include_directories (boost_fiber_numa BEFORE PUBLIC ${Boost_INCLUDE_DIRS})

add_library(Boost::fiber_numa ALIAS boost_fiber_numa)

target_compile_definitions(boost_fiber_numa
  PUBLIC BOOST_FIBER_NO_LIB
  PRIVATE BOOST_FIBER_SOURCE BOOST_FIBERS_SOURCE
)

target_no_warning(boost_fiber_numa unused-but-set-variable)

if(BUILD_SHARED_LIBS)
  target_compile_definitions(boost_fiber_numa PUBLIC BOOST_FIBER_DYN_LINK BOOST_FIBERS_DYN_LINK)
else()
  target_compile_definitions(boost_fiber_numa PUBLIC BOOST_FIBER_STATIC_LINK)
endif()

