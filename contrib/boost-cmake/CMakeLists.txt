# Supported contrib/boost source variants:
# 1. Default - Minimized vrsion from release archive : https://github.com/ClickHouse-Extras/boost
# 2. Release archive unpacked to contrib/boost
# 3. Full boost https://github.com/boostorg/boost

# if boostorg/boost connected as submodule: Update all boost internal submodules to tag:
# git submodule foreach "git fetch --all && git checkout boost-1.66.0 || true"

#
# Important boost patch: 094c18b
#

set (LIBRARY_DIR ${ClickHouse_SOURCE_DIR}/contrib/boost)

if (NOT MSVC)
    add_definitions(-Wno-unused-variable -Wno-deprecated-declarations)
endif ()


file(GLOB_RECURSE BOOST_INTERNAL_SRC ${LIBRARY_DIR}/libs/*/src/*.cpp)

add_library(boost_internal STATIC ${BOOST_INTERNAL_SRC})

target_include_directories (boost_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})

target_compile_definitions (boost_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
