find_program (OBJCOPY_PATH NAMES "objcopy")
find_program (LLVM_OBJCOPY_PATH NAMES "llvm-objcopy${COMPILER_POSTFIX}" "llvm-objcopy")

if (LLVM_OBJCOPY_PATH)
    set(CMAKE_OBJCOPY ${LLVM_OBJCOPY_PATH})
else ()
    set(CMAKE_OBJCOPY ${OBJCOPY_PATH})
endif ()

message(STATUS "executing: ${CMAKE_OBJCOPY} --compress-debug-sections=zlib-gnu ${CMAKE_INSTALL_PREFIX}/tiflash")
execute_process(COMMAND ${CMAKE_OBJCOPY} --compress-debug-sections=zlib-gnu ${CMAKE_INSTALL_PREFIX}/tiflash)