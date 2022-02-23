set(CMAKE_INSTALL_NAME_TOOL "install_name_tool")

message(STATUS "renaming tiflash-proxy library")
file(RENAME ${CMAKE_INSTALL_PREFIX}/libraftstore_proxy.dylib ${CMAKE_INSTALL_PREFIX}/libtiflash_proxy.dylib)

message(STATUS "${CMAKE_INSTALL_NAME_TOOL} -id @rpath/libtiflash_proxy.dylib ${CMAKE_INSTALL_PREFIX}/libtiflash_proxy.dylib")
execute_process(COMMAND ${CMAKE_INSTALL_NAME_TOOL} -id @rpath/libtiflash_proxy.dylib ${CMAKE_INSTALL_PREFIX}/libtiflash_proxy.dylib)

execute_process(COMMAND sh -c "otool -L ${CMAKE_INSTALL_PREFIX}/tiflash | grep proxy | awk '{print \$1;}'" OUTPUT_VARIABLE OLD_DEPENDENCY)
message(STATUS "${CMAKE_INSTALL_NAME_TOOL} -change ${OLD_DEPENDENCY} @executable_path/libtiflash_proxy.dylib ${CMAKE_INSTALL_PREFIX}/tiflash")
execute_process(COMMAND ${CMAKE_INSTALL_NAME_TOOL} -change ${OLD_DEPENDENCY} @executable_path/libtiflash_proxy.dylib ${CMAKE_INSTALL_PREFIX}/tiflash)