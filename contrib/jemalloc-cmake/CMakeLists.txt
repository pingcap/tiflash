option (JEMALLOC_SUBMODULE "Use jemalloc submodule instead of ep" OFF)

if (JEMALLOC_SUBMODULE)
if (NOT CMAKE_SYSTEM_NAME MATCHES "Linux")
    message (WARNING "jemalloc support on non-linux is EXPERIMENTAL")
endif()

set(JEMALLOC_SOURCE_DIR ${TiFlash_SOURCE_DIR}/contrib/jemalloc)

set (SRCS
        "${JEMALLOC_SOURCE_DIR}/src/arena.c"
        "${JEMALLOC_SOURCE_DIR}/src/background_thread.c"
        "${JEMALLOC_SOURCE_DIR}/src/base.c"
        "${JEMALLOC_SOURCE_DIR}/src/bin.c"
        "${JEMALLOC_SOURCE_DIR}/src/bin_info.c"
        "${JEMALLOC_SOURCE_DIR}/src/bitmap.c"
        "${JEMALLOC_SOURCE_DIR}/src/buf_writer.c"
        "${JEMALLOC_SOURCE_DIR}/src/cache_bin.c"
        "${JEMALLOC_SOURCE_DIR}/src/ckh.c"
        "${JEMALLOC_SOURCE_DIR}/src/counter.c"
        "${JEMALLOC_SOURCE_DIR}/src/ctl.c"
        "${JEMALLOC_SOURCE_DIR}/src/decay.c"
        "${JEMALLOC_SOURCE_DIR}/src/div.c"
        "${JEMALLOC_SOURCE_DIR}/src/ecache.c"
        "${JEMALLOC_SOURCE_DIR}/src/edata.c"
        "${JEMALLOC_SOURCE_DIR}/src/edata_cache.c"
        "${JEMALLOC_SOURCE_DIR}/src/ehooks.c"
        "${JEMALLOC_SOURCE_DIR}/src/emap.c"
        "${JEMALLOC_SOURCE_DIR}/src/eset.c"
        "${JEMALLOC_SOURCE_DIR}/src/exp_grow.c"
        "${JEMALLOC_SOURCE_DIR}/src/extent.c"
        "${JEMALLOC_SOURCE_DIR}/src/extent_dss.c"
        "${JEMALLOC_SOURCE_DIR}/src/extent_mmap.c"
        "${JEMALLOC_SOURCE_DIR}/src/fxp.c"
        "${JEMALLOC_SOURCE_DIR}/src/hook.c"
        "${JEMALLOC_SOURCE_DIR}/src/hpa.c"
        "${JEMALLOC_SOURCE_DIR}/src/hpa_hooks.c"
        "${JEMALLOC_SOURCE_DIR}/src/hpdata.c"
        "${JEMALLOC_SOURCE_DIR}/src/inspect.c"
        "${JEMALLOC_SOURCE_DIR}/src/jemalloc.c"
        "${JEMALLOC_SOURCE_DIR}/src/large.c"
        "${JEMALLOC_SOURCE_DIR}/src/log.c"
        "${JEMALLOC_SOURCE_DIR}/src/malloc_io.c"
        "${JEMALLOC_SOURCE_DIR}/src/mutex.c"
        "${JEMALLOC_SOURCE_DIR}/src/nstime.c"
        "${JEMALLOC_SOURCE_DIR}/src/pa.c"
        "${JEMALLOC_SOURCE_DIR}/src/pac.c"
        "${JEMALLOC_SOURCE_DIR}/src/pa_extra.c"
        "${JEMALLOC_SOURCE_DIR}/src/pages.c"
        "${JEMALLOC_SOURCE_DIR}/src/pai.c"
        "${JEMALLOC_SOURCE_DIR}/src/peak_event.c"
        "${JEMALLOC_SOURCE_DIR}/src/prof.c"
        "${JEMALLOC_SOURCE_DIR}/src/prof_data.c"
        "${JEMALLOC_SOURCE_DIR}/src/prof_log.c"
        "${JEMALLOC_SOURCE_DIR}/src/prof_recent.c"
        "${JEMALLOC_SOURCE_DIR}/src/prof_stats.c"
        "${JEMALLOC_SOURCE_DIR}/src/prof_sys.c"
        "${JEMALLOC_SOURCE_DIR}/src/psset.c"
        "${JEMALLOC_SOURCE_DIR}/src/rtree.c"
        "${JEMALLOC_SOURCE_DIR}/src/safety_check.c"
        "${JEMALLOC_SOURCE_DIR}/src/san_bump.c"
        "${JEMALLOC_SOURCE_DIR}/src/san.c"
        "${JEMALLOC_SOURCE_DIR}/src/sc.c"
        "${JEMALLOC_SOURCE_DIR}/src/sec.c"
        "${JEMALLOC_SOURCE_DIR}/src/stats.c"
        "${JEMALLOC_SOURCE_DIR}/src/sz.c"
        "${JEMALLOC_SOURCE_DIR}/src/tcache.c"
        "${JEMALLOC_SOURCE_DIR}/src/test_hooks.c"
        "${JEMALLOC_SOURCE_DIR}/src/thread_event.c"
        "${JEMALLOC_SOURCE_DIR}/src/ticker.c"
        "${JEMALLOC_SOURCE_DIR}/src/tsd.c"
        "${JEMALLOC_SOURCE_DIR}/src/witness.c"
)

if(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  list(APPEND SRCS ${JEMALLOC_SOURCE_DIR}/src/zone.c)
endif()

if (ARCH_LINUX)
    # ThreadPool select job randomly, and there can be some threads that had been
    # performed some memory heavy task before and will be inactive for some time,
    # but until it will became active again, the memory will not be freed since by
    # default each thread has it's own arena, but there should be not more then
    # 4*CPU arenas (see opt.nareans description).
    #
    # By enabling percpu_arena number of arenas limited to number of CPUs and hence
    # this problem should go away.
    #
    # muzzy_decay_ms -- use MADV_FREE when available on newer Linuxes, to
    # avoid spurious latencies and additional work associated with
    # MADV_DONTNEED. See
    # https://github.com/ClickHouse/ClickHouse/issues/11121 for motivation.
    if (JEMALLOC_NARENAS GREATER 0)
        set (JEMALLOC_CONFIG_MALLOC_CONF "percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000,narenas:${JEMALLOC_NARENAS}")
    else ()
        set (JEMALLOC_CONFIG_MALLOC_CONF "percpu_arena:percpu,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000")
    endif ()
else ()
    set (JEMALLOC_CONFIG_MALLOC_CONF "invalid:invalid,stats_print:true,oversize_threshold:0,muzzy_decay_ms:5000,dirty_decay_ms:5000")
endif ()

message (STATUS "jemalloc malloc_conf: ${JEMALLOC_CONFIG_MALLOC_CONF}")

if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -w")
endif ()

add_library(jemalloc STATIC ${SRCS})
set (JEMALLOC_INCLUDE_PREFIX)

if (OS_LINUX)
    set (JEMALLOC_INCLUDE_PREFIX "include_linux")
    target_compile_definitions(jemalloc PRIVATE JEMALLOC_MADV_FREE=8)
elseif (OS_FREEBSD)
    set (JEMALLOC_INCLUDE_PREFIX "include_freebsd")
elseif (APPLE)
    set (JEMALLOC_INCLUDE_PREFIX "include_darwin")
else ()
    message (FATAL_ERROR "internal jemalloc: This OS is not supported")
endif ()

if (APPLE)
        set (JEMALLOC_PREFIX "je_")
        set (JEMALLOC_CPREFIX "JE_")
        endif ()

if (ARCH_AMD64)
    if (USE_MUSL)
        set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_x86_64_musl")
    else()
        set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_x86_64")
    endif()
elseif (ARCH_AARCH64)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_aarch64")
elseif (ARCH_PPC64LE)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_ppc64le")
elseif (ARCH_RISCV64)
    set(JEMALLOC_INCLUDE_PREFIX "${JEMALLOC_INCLUDE_PREFIX}_riscv64")
else ()
    message (FATAL_ERROR "internal jemalloc: This arch is not supported")
endif ()

configure_file(${JEMALLOC_INCLUDE_PREFIX}/jemalloc/internal/jemalloc_internal_defs.h.in
        ${JEMALLOC_INCLUDE_PREFIX}/jemalloc/internal/jemalloc_internal_defs.h)
configure_file(include/jemalloc/jemalloc_rename.h.in
        include/jemalloc/jemalloc_rename.h)

target_include_directories(jemalloc SYSTEM PRIVATE
        "${CMAKE_CURRENT_BINARY_DIR}/${JEMALLOC_INCLUDE_PREFIX}/jemalloc/internal")

target_include_directories(jemalloc PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/include/jemalloc" ${JEMALLOC_SOURCE_DIR}/include ${TiFlash_SOURCE_DIR}/contrib/jemalloc-cmake/include)

target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_NO_PRIVATE_NAMESPACE)

if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_DEBUG=1)
endif ()

# jemalloc profiling
if (ENABLE_JEMALLOC_PROF)
    target_compile_definitions(jemalloc PRIVATE -DJEMALLOC_PROF=1)
    if (USE_UNWIND)
        target_compile_definitions (jemalloc PRIVATE -DJEMALLOC_PROF_LIBUNWIND=1)
        target_include_directories (jemalloc BEFORE PRIVATE ${UNWIND_INCLUDE_DIR})
        target_include_directories (jemalloc BEFORE PRIVATE ${UNWIND_INCREMENTAL_DIR})
        target_link_libraries (jemalloc PRIVATE ${UNWIND_LIBRARY})
    endif ()
endif ()

target_compile_options(jemalloc PRIVATE -D_GNU_SOURCE)
  return()
endif()

include(ExternalProject)

set(JEMALLOC_BUILD_VERSION "5.3.0")
set(JEMALLOC_BUILD_SHA256_CHECKSUM "2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa")
  set(JEMALLOC_SOURCE_URL
           "https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_BUILD_VERSION}/jemalloc-${JEMALLOC_BUILD_VERSION}.tar.bz2")

set(EP_LIST_SEPARATOR "|")
set(EP_COMMON_OPTIONS LIST_SEPARATOR ${EP_LIST_SEPARATOR})
list(APPEND
       EP_COMMON_OPTIONS
       LOG_CONFIGURE
       1
       LOG_BUILD
       1
       LOG_INSTALL
       1
       LOG_DOWNLOAD
       1
       LOG_OUTPUT_ON_FAILURE
       1)

           find_program(MAKE make)
       set(MAKE_BUILD_ARGS "-j6")

macro(build_jemalloc)
  # Our build of jemalloc is specially prefixed so that it will not
  # conflict with the default allocator as well as other jemalloc
  # installations.

  message(STATUS "Building jemalloc from source")

  set(JEMALLOC_PREFIX
      "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src/jemalloc_ep/dist/")
  set(JEMALLOC_LIB_DIR "${JEMALLOC_PREFIX}/lib")
  set(JEMALLOC_STATIC_LIB
      "${JEMALLOC_LIB_DIR}/libjemalloc_pic${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(JEMALLOC_CONFIGURE_COMMAND ./configure "AR=${CMAKE_AR}" "CC=${CMAKE_C_COMPILER}")
  if(CMAKE_OSX_SYSROOT)
    list(APPEND JEMALLOC_CONFIGURE_COMMAND "SDKROOT=${CMAKE_OSX_SYSROOT}")
  endif()
  if(DEFINED ARROW_JEMALLOC_LG_PAGE)
    # Used for arm64 manylinux wheels in order to make the wheel work on both
    # 4k and 64k page arm64 systems.
    list(APPEND JEMALLOC_CONFIGURE_COMMAND "--with-lg-page=16")
  endif()
  list(APPEND
       JEMALLOC_CONFIGURE_COMMAND
       "--prefix=${JEMALLOC_PREFIX}"
       "--libdir=${JEMALLOC_LIB_DIR}"
    #    "--with-jemalloc-prefix=je_"
    #    "--with-private-namespace=je_private_"
       # "--with-malloc-conf=stats_print:true"
       "--without-export"
       "--disable-shared"
       # Don't override operator new()
       "--disable-cxx"
       "--disable-libdl"
       # See https://github.com/jemalloc/jemalloc/issues/1237
       "--disable-initial-exec-tls")
    # Enable jemalloc debug checks when Arrow itself has debugging enabled
    # list(APPEND JEMALLOC_CONFIGURE_COMMAND "--enable-debug")

  set(JEMALLOC_BUILD_COMMAND ${MAKE} ${MAKE_BUILD_ARGS})

  if(CMAKE_OSX_SYSROOT)
    list(APPEND JEMALLOC_BUILD_COMMAND "SDKROOT=${CMAKE_OSX_SYSROOT}")
    # list(APPEND JEMALLOC_BUILD_COMMAND "-isysroot ${CMAKE_OSX_SYSROOT}")
  endif()

  externalproject_add(jemalloc_ep
                      ${EP_COMMON_OPTIONS}
                      URL ${JEMALLOC_SOURCE_URL}
                      URL_HASH "SHA256=${JEMALLOC_BUILD_SHA256_CHECKSUM}"
                      PATCH_COMMAND touch doc/jemalloc.3 doc/jemalloc.html
                                    # The prefix "je_arrow_" must be kept in sync with the value in memory_pool.cc
                      CONFIGURE_COMMAND ${JEMALLOC_CONFIGURE_COMMAND}
                      BUILD_IN_SOURCE 1
                      BUILD_COMMAND ${JEMALLOC_BUILD_COMMAND}
                      BUILD_BYPRODUCTS "${JEMALLOC_STATIC_LIB}"
                      INSTALL_COMMAND ${MAKE} -j1 install)

  # Don't use the include directory directly so that we can point to a path
  # that is unique to our codebase.
  set(JEMALLOC_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/jemalloc_ep-prefix/src")
  # The include directory must exist before it is referenced by a target.
  file(MAKE_DIRECTORY "${JEMALLOC_INCLUDE_DIR}")
  add_library(jemalloc STATIC IMPORTED GLOBAL)
  set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION
                                                      "${JEMALLOC_STATIC_LIB}")
#   set_target_properties(jemalloc PROPERTIES INTERFACE_INCLUDE_DIRECTORIES "${JEMALLOC_INCLUDE_DIR}")

  target_include_directories(jemalloc BEFORE
                             PUBLIC INTERFACE "${JEMALLOC_INCLUDE_DIR}")
  add_dependencies(jemalloc jemalloc_ep)

  set(jemalloc_VENDORED TRUE)
  # For config.h.cmake
  set(ARROW_JEMALLOC_VENDORED ${jemalloc_VENDORED})
endmacro()

build_jemalloc()
