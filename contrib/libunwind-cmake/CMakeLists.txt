enable_language(ASM)

set(LIBUNWIND_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/libunwind")

add_library(unwind
${LIBUNWIND_SOURCE_DIR}/src/mi/init.c
${LIBUNWIND_SOURCE_DIR}/src/mi/flush_cache.c
${LIBUNWIND_SOURCE_DIR}/src/mi/mempool.c
${LIBUNWIND_SOURCE_DIR}/src/mi/strerror.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/is_fpreg.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/regname.c
${LIBUNWIND_SOURCE_DIR}/src/mi/_ReadULEB.c
${LIBUNWIND_SOURCE_DIR}/src/mi/_ReadSLEB.c
${LIBUNWIND_SOURCE_DIR}/src/mi/backtrace.c
${LIBUNWIND_SOURCE_DIR}/src/mi/dyn-cancel.c
${LIBUNWIND_SOURCE_DIR}/src/mi/dyn-info-list.c
${LIBUNWIND_SOURCE_DIR}/src/mi/dyn-register.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Ldyn-extract.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lfind_dynamic_proc_info.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lget_accessors.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lget_proc_info_by_ip.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lget_proc_name.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lput_dynamic_unwind_info.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Ldestroy_addr_space.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lget_reg.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lset_reg.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lget_fpreg.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lset_fpreg.c
${LIBUNWIND_SOURCE_DIR}/src/mi/Lset_caching_policy.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/setcontext.S
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Lcreate_addr_space.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Lget_save_loc.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Lglobal.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Linit.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Linit_local.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Linit_remote.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Lget_proc_info.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Lregs.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Lresume.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Lstash_frame.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Lstep.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Ltrace.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/getcontext.S
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lexpr.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lfde.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lfind_proc_info-lsb.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lparser.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lpe.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/global.c
${LIBUNWIND_SOURCE_DIR}/src/elf64.c

${LIBUNWIND_SOURCE_DIR}/src/os-linux.c
${LIBUNWIND_SOURCE_DIR}/src/x86_64/Los-linux.c
)

add_definitions(-DHAVE_CONFIG_H -DHAVE_LINK_H -D_GNU_SOURCE)
find_file (HAVE_ATOMIC_OPS_H "atomic_ops.h")

configure_file(${LIBUNWIND_SOURCE_DIR}/include/config.h.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/include/config.h)
configure_file(${LIBUNWIND_SOURCE_DIR}/include/libunwind-common.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/libunwind-common.h)
configure_file(${LIBUNWIND_SOURCE_DIR}/include/libunwind.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/libunwind.h)
configure_file(${LIBUNWIND_SOURCE_DIR}/include/tdep/libunwind_i.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/tdep/libunwind_i.h)

target_compile_definitions (unwind PRIVATE HAVE_CONFIG_H=1 _XOPEN_SOURCE _GNU_SOURCE)
target_compile_options (unwind PRIVATE -Wno-visibility -Wno-header-guard)

target_include_directories (unwind PUBLIC ${LIBUNWIND_SOURCE_DIR}/include)
target_include_directories (unwind PRIVATE ${LIBUNWIND_SOURCE_DIR}/include/tdep)
target_include_directories (unwind PRIVATE ${LIBUNWIND_SOURCE_DIR}/src)
target_include_directories (unwind PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/include)
