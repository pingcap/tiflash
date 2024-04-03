enable_language(ASM)

set(LIBUNWIND_SOURCE_DIR "${TiFlash_SOURCE_DIR}/contrib/libunwind")
if(OS_LINUX)
    set(UNWIND_OS "linux")
elseif(OS_FREEBSD)
    set(UNWIND_OS "freebsd")
endif()

if(ARCH_AARCH64)
    set(UNWIND_ARCH "aarch64")
    file(GLOB UNWIND_ARCH_SRC
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lapply_reg_state.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lcreate_addr_space.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lget_proc_info.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lget_save_loc.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lglobal.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit_local.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit_remote.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lis_signal_frame.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lreg_states_iterate.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lregs.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lresume.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lstash_frame.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lstep.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Ltrace.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/gen-offsets.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/getcontext.S
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/is_fpreg.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/longjmp.S
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/regname.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/siglongjmp.S)
elseif(ARCH_ARM)
    set(UNWIND_ARCH "arm")
    file(GLOB UNWIND_ARCH_SRC
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lapply_reg_state.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lcreate_addr_space.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lex_tables.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lget_proc_info.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lget_save_loc.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lglobal.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit_local.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit_remote.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lis_signal_frame.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Los-${UNWIND_OS}.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lreg_states_iterate.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lregs.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lresume.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lstash_frame.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lstep.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Ltrace.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/gen-offsets.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/getcontext.S
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/is_fpreg.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/longjmp.S
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/regname.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/siglongjmp.S)
elseif(ARCH_I386)
    set(UNWIND_ARCH "x86")
    file(GLOB UNWIND_ARCH_SRC
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lapply_reg_state.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lcreate_addr_space.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lget_proc_info.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lget_save_loc.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lglobal.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit_local.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit_remote.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Los-${UNWIND_OS}.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lreg_states_iterate.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lregs.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lresume.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lstep.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/is_fpreg.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/longjmp.S
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/regname.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/siglongjmp.S
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/getcontext-${UNWIND_OS}.S)
else()
    set(UNWIND_ARCH "x86_64")
    file(GLOB UNWIND_ARCH_SRC
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/is_fpreg.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/regname.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/setcontext.S
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lcreate_addr_space.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lget_save_loc.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lglobal.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit_local.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Linit_remote.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lget_proc_info.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lregs.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lresume.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lstash_frame.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Lstep.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Ltrace.c
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/getcontext.S
    ${LIBUNWIND_SOURCE_DIR}/src/${UNWIND_ARCH}/Los-${UNWIND_OS}.c)
endif()


file(GLOB UNWIND_COMMON_SRC
${LIBUNWIND_SOURCE_DIR}/src/mi/init.c
${LIBUNWIND_SOURCE_DIR}/src/mi/flush_cache.c
${LIBUNWIND_SOURCE_DIR}/src/mi/mempool.c
${LIBUNWIND_SOURCE_DIR}/src/mi/strerror.c
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
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lexpr.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lfde.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lfind_proc_info-lsb.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lparser.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/Lpe.c
${LIBUNWIND_SOURCE_DIR}/src/dwarf/global.c
${LIBUNWIND_SOURCE_DIR}/src/elf64.c
${LIBUNWIND_SOURCE_DIR}/src/os-${UNWIND_OS}.c
)


add_library(unwind ${UNWIND_COMMON_SRC} ${UNWIND_ARCH_SRC})

add_definitions(-DHAVE_CONFIG_H -DHAVE_LINK_H -D_GNU_SOURCE)
find_file (HAVE_ATOMIC_OPS_H "atomic_ops.h")

configure_file(${LIBUNWIND_SOURCE_DIR}/include/config.h.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/include/config.h)
configure_file(${LIBUNWIND_SOURCE_DIR}/include/libunwind-common.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/libunwind-common.h)
configure_file(${LIBUNWIND_SOURCE_DIR}/include/libunwind.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/libunwind.h)
configure_file(${LIBUNWIND_SOURCE_DIR}/include/tdep/libunwind_i.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/tdep/libunwind_i.h)

target_compile_definitions (unwind PRIVATE HAVE_CONFIG_H=1 _XOPEN_SOURCE _GNU_SOURCE)
target_compile_options (unwind PRIVATE -Wno-visibility -Wno-header-guard -Wno-incompatible-pointer-types -Wno-format)

target_include_directories (unwind PUBLIC ${LIBUNWIND_SOURCE_DIR}/include)
target_include_directories (unwind PRIVATE ${LIBUNWIND_SOURCE_DIR}/include/tdep)
target_include_directories (unwind PRIVATE ${LIBUNWIND_SOURCE_DIR}/src)
target_include_directories (unwind PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/include)
target_compile_options (unwind PRIVATE "-Wno-single-bit-bitfield-constant-conversion")
