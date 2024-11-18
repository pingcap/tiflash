include(ExternalProject)

# hdf5 is too large. Instead of adding as a submodule, let's simply download from GitHub.
ExternalProject_Add(hdf5-external
    PREFIX          ${CMAKE_CURRENT_BINARY_DIR}
    DOWNLOAD_DIR    ${TiFlash_SOURCE_DIR}/contrib/hdf5-cmake/download
    URL             https://github.com/HDFGroup/hdf5/archive/refs/tags/hdf5_1.14.4.3.zip
    URL_HASH        MD5=bc987d22e787290127aacd7b99b4f31e
    CMAKE_ARGS
        -DCMAKE_BUILD_TYPE=Release
        -DCMAKE_INSTALL_PREFIX=<INSTALL_DIR>
        -DBUILD_STATIC_LIBS=ON
        -DBUILD_SHARED_LIBS=OFF
        -DBUILD_TESTING=OFF
        -DHDF5_BUILD_HL_LIB=OFF
        -DHDF5_BUILD_TOOLS=OFF
        -DHDF5_BUILD_CPP_LIB=ON
        -DHDF5_BUILD_EXAMPLES=OFF
        -DHDF5_ENABLE_Z_LIB_SUPPORT=OFF
        -DHDF5_ENABLE_SZIP_SUPPORT=OFF
    BUILD_BYPRODUCTS           <INSTALL_DIR>/lib/${CMAKE_FIND_LIBRARY_PREFIXES}hdf5.a  # Workaround for Ninja
    USES_TERMINAL_DOWNLOAD     TRUE
    USES_TERMINAL_CONFIGURE    TRUE
    USES_TERMINAL_BUILD        TRUE
    USES_TERMINAL_INSTALL      TRUE
    EXCLUDE_FROM_ALL           TRUE
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)

ExternalProject_Get_Property(hdf5-external INSTALL_DIR)

add_library(tiflash_contrib::hdf5 STATIC IMPORTED GLOBAL)
set_target_properties(tiflash_contrib::hdf5 PROPERTIES
    IMPORTED_LOCATION       ${INSTALL_DIR}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}hdf5.a
)
add_dependencies(tiflash_contrib::hdf5 hdf5-external)

file(MAKE_DIRECTORY ${INSTALL_DIR}/include)
target_include_directories(tiflash_contrib::hdf5 SYSTEM INTERFACE
    ${INSTALL_DIR}/include
)
