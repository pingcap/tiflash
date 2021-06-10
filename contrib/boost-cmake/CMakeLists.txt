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

file(GLOB boost_program_options_src ${LIBRARY_DIR}/program_options/src/*.cpp)
add_library(boost_program_options_internal ${boost_program_options_src})

file(GLOB boost_filesystem_src ${LIBRARY_DIR}/boost_filesystem/src/*.cpp)
add_library(boost_filesystem_internal ${boost_filesystem_src})

add_library(boost_system_internal ${LIBRARY_DIR}/system/src/error_code.cpp)

target_include_directories (boost_program_options_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_filesystem_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})
target_include_directories (boost_system_internal BEFORE PUBLIC ${Boost_INCLUDE_DIRS})

target_compile_definitions (boost_program_options_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
target_compile_definitions (boost_filesystem_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
target_compile_definitions (boost_system_internal PUBLIC BOOST_SYSTEM_NO_DEPRECATED)
