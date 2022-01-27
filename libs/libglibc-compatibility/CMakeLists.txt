enable_language (ASM)

add_library (glibc-compatibility glibc-compatibility.c
musl/pipe2.c
musl/fallocate.c
musl/longjmp.s
musl/vasprintf.c
musl/lgamma.c
musl/posix_spawn.c
musl/futimens.c
musl/syscall.s
musl/syscall_ret.c
musl/sched_cpucount.c)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    set_source_files_properties(musl/longjmp.s musl/syscall.s
            PROPERTIES COMPILE_FLAGS -Wno-unused-command-line-argument)
endif()
add_subdirectory (tests EXCLUDE_FROM_ALL)
