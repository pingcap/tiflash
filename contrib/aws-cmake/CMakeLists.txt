# Utilities.
include("${TiFlash_SOURCE_DIR}/contrib/aws-cmake/AwsFeatureTests.cmake")
include("${TiFlash_SOURCE_DIR}/contrib/aws-cmake/AwsThreadAffinity.cmake")
include("${TiFlash_SOURCE_DIR}/contrib/aws-cmake/AwsThreadName.cmake")
include("${TiFlash_SOURCE_DIR}/contrib/aws-cmake/AwsSIMD.cmake")

# Gather sources and options.
set(AWS_SOURCES)
set(AWS_PUBLIC_INCLUDES)
set(AWS_PRIVATE_INCLUDES)
set(AWS_PUBLIC_COMPILE_DEFS)
set(AWS_PRIVATE_COMPILE_DEFS)
set(AWS_PRIVATE_LIBS)

# Versions. Note that we may update the patch under /aws-cmake to make it work
# after upgrading the aws-sdk-cpp version.
list(APPEND AWS_PUBLIC_COMPILE_DEFS "-DAWS_SDK_VERSION_MAJOR=1")
list(APPEND AWS_PUBLIC_COMPILE_DEFS "-DAWS_SDK_VERSION_MINOR=11")
list(APPEND AWS_PUBLIC_COMPILE_DEFS "-DAWS_SDK_VERSION_PATCH=186")

# ignore the symbols defined in `aws/common/external/ittnotify.h`
list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DENABLE_CURL_CLIENT -DINTEL_NO_ITTNOTIFY_API")

if (CMAKE_BUILD_TYPE_UC STREQUAL "DEBUG")
    list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DDEBUG_BUILD")
endif()

set(ENABLE_OPENSSL_ENCRYPTION ON)
if (ENABLE_OPENSSL_ENCRYPTION)
    list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DENABLE_OPENSSL_ENCRYPTION")
endif()

set(USE_S2N ON)
if (USE_S2N)
    list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DUSE_S2N")
endif()

# Directories.
SET(AWS_SDK_DIR "${TiFlash_SOURCE_DIR}/contrib/aws")
SET(AWS_SDK_CORE_DIR "${AWS_SDK_DIR}/src/aws-cpp-sdk-core")
SET(AWS_SDK_S3_DIR "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-s3")
SET(AWS_SDK_STS_DIR "${AWS_SDK_DIR}/generated/src/aws-cpp-sdk-sts")

SET(AWS_AUTH_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-auth")
SET(AWS_CAL_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-cal")
SET(AWS_CHECKSUMS_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-checksums")
SET(AWS_COMMON_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-common")
SET(AWS_COMPRESSION_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-compression")
SET(AWS_CRT_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-crt-cpp")
SET(AWS_EVENT_STREAM_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-event-stream")
SET(AWS_HTTP_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-http")
SET(AWS_IO_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-io")
SET(AWS_MQTT_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-mqtt")
SET(AWS_S2N_TLS_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-s2n-tls")
SET(AWS_S3_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-s3")
SET(AWS_SDKUTILS_DIR "${TiFlash_SOURCE_DIR}/contrib/aws-c-sdkutils")

# aws-cpp-sdk-core
file(GLOB AWS_SDK_CORE_SRC
    "${AWS_SDK_CORE_DIR}/source/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/auth/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/auth/bearer-token-provider/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/auth/signer/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/auth/signer-provider/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/client/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/config/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/config/defaults/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/endpoint/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/endpoint/internal/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/external/cjson/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/external/tinyxml2/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/http/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/http/curl/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/http/standard/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/internal/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/monitoring/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/smithy/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/smithy/tracing/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/base64/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/component-registry/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/crypto/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/crypto/factory/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/crypto/openssl/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/event/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/json/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/logging/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/memory/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/memory/stl/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/stream/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/threading/*.cpp"
    "${AWS_SDK_CORE_DIR}/source/utils/xml/*.cpp"
)

execute_process(
  COMMAND grep "GetResponseBody().peek()" "${AWS_SDK_CORE_DIR}/source/client/AWSXmlClient.cpp"
  RESULT_VARIABLE HAVE_APPLY_PATCH)
# grep - Normally, the exit status is 0 if selected lines are found and 1 otherwise. But the exit status is 2 if an error occurred.
if (HAVE_APPLY_PATCH EQUAL 1)
  message(STATUS "aws patch not apply: ${HAVE_APPLY_PATCH}, patching...")
  ## update the patch using `git format-patch` if you upgrade aws
  set (AWS_PATCH_FILE "${TiFlash_SOURCE_DIR}/contrib/aws-cmake/0001-More-reliable-way-to-check-if-there-is-anything-in-r.patch")
  # apply the patch
  execute_process(
    COMMAND git apply -v "${AWS_PATCH_FILE}"
    WORKING_DIRECTORY "${AWS_SDK_DIR}"
    COMMAND_ECHO STDOUT
    RESULT_VARIABLE PATCH_SUCC)
  if (NOT PATCH_SUCC EQUAL 0)
    message(FATAL_ERROR "Can not apply aws patch ${AWS_PATCH_FILE}")
  endif ()

  set (AWS_PATCH_FILE "${TiFlash_SOURCE_DIR}/contrib/aws-cmake/0002-Reduce-verbose-error-logging-and-404-for-HEAD-reques.patch")
  # apply the patch
  execute_process(
    COMMAND git apply -v "${AWS_PATCH_FILE}"
    WORKING_DIRECTORY "${AWS_SDK_DIR}"
    COMMAND_ECHO STDOUT
    RESULT_VARIABLE PATCH_SUCC)
  if (NOT PATCH_SUCC EQUAL 0)
    message(FATAL_ERROR "Can not apply aws patch ${AWS_PATCH_FILE}")
  else ()
    message(STATUS "aws patch done")
  endif ()
elseif (HAVE_APPLY_PATCH EQUAL 0)
  message(STATUS "aws patch have been applied: ${HAVE_APPLY_PATCH}")
else ()
  message(FATAL_ERROR "Can not check the aws patch status")
endif ()

if(OS_LINUX OR OS_DARWIN)
    file(GLOB AWS_SDK_CORE_NET_SRC "${AWS_SDK_CORE_DIR}/source/net/linux-shared/*.cpp")
    file(GLOB AWS_SDK_CORE_PLATFORM_SRC "${AWS_SDK_CORE_DIR}/source/platform/linux-shared/*.cpp")
else()
    file(GLOB AWS_SDK_CORE_NET_SRC "${AWS_SDK_CORE_DIR}/source/net/*.cpp")
    set(AWS_SDK_CORE_PLATFORM_SRC)
endif()

OPTION(USE_AWS_MEMORY_MANAGEMENT "Aws memory management" OFF)
configure_file("${AWS_SDK_CORE_DIR}/include/aws/core/SDKConfig.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/aws/core/SDKConfig.h" @ONLY)

list(APPEND AWS_SOURCES ${AWS_SDK_CORE_SRC} ${AWS_SDK_CORE_NET_SRC} ${AWS_SDK_CORE_PLATFORM_SRC})

list(APPEND AWS_PUBLIC_INCLUDES
    "${AWS_SDK_CORE_DIR}/include/"
    "${CMAKE_CURRENT_BINARY_DIR}/include"
)


# aws-cpp-sdk-s3
file(GLOB AWS_SDK_S3_SRC
    "${AWS_SDK_S3_DIR}/source/*.cpp"
    "${AWS_SDK_S3_DIR}/source/model/*.cpp"
)

list(APPEND AWS_SOURCES ${AWS_SDK_S3_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_SDK_S3_DIR}/include/")

# aws-cpp-sdk-sts
file(GLOB AWS_SDK_STS_SRC
    "${AWS_SDK_STS_DIR}/source/*.cpp"
    "${AWS_SDK_STS_DIR}/source/model/*.cpp"
)

list(APPEND AWS_SOURCES ${AWS_SDK_STS_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_SDK_STS_DIR}/include/")

# aws-c-auth
file(GLOB AWS_AUTH_SRC
    "${AWS_AUTH_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_AUTH_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_AUTH_DIR}/include/")


# aws-c-cal
file(GLOB AWS_CAL_SRC
    "${AWS_CAL_DIR}/source/*.c"
)

if (ENABLE_OPENSSL_ENCRYPTION)
    # Note: We use `unix/*.c` for both OS_LINUX and OS_DARWIN
    # `darwin/*.c` lead to compile errors
    file(GLOB AWS_CAL_OS_SRC
        "${AWS_CAL_DIR}/source/unix/*.c"
    )
    list(APPEND AWS_PRIVATE_LIBS OpenSSL::Crypto)
endif()

list(APPEND AWS_SOURCES ${AWS_CAL_SRC} ${AWS_CAL_OS_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_CAL_DIR}/include/")


# aws-c-event-stream
file(GLOB AWS_EVENT_STREAM_SRC
    "${AWS_EVENT_STREAM_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_EVENT_STREAM_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_EVENT_STREAM_DIR}/include/")


# aws-c-common
file(GLOB AWS_COMMON_SRC
    "${AWS_COMMON_DIR}/source/*.c"
    "${AWS_COMMON_DIR}/source/external/*.c"
    "${AWS_COMMON_DIR}/source/posix/*.c"
)

file(GLOB AWS_COMMON_ARCH_SRC
    "${AWS_COMMON_DIR}/source/arch/generic/*.c"
)

if (AWS_ARCH_INTEL)
    file(GLOB AWS_COMMON_ARCH_SRC
        "${AWS_COMMON_DIR}/source/arch/intel/cpuid.c"
        "${AWS_COMMON_DIR}/source/arch/intel/asm/*.c"
    )
elseif (AWS_ARCH_ARM64 OR AWS_ARCH_ARM32)
    if (AWS_HAVE_AUXV)
        file(GLOB AWS_COMMON_ARCH_SRC
            "${AWS_COMMON_DIR}/source/arch/arm/asm/*.c"
        )
    endif()
endif()

set(AWS_COMMON_AVX2_SRC)
if (HAVE_AVX2_INTRINSICS)
    list(APPEND AWS_PRIVATE_COMPILE_DEFS "-DUSE_SIMD_ENCODING")
    set(AWS_COMMON_AVX2_SRC "${AWS_COMMON_DIR}/source/arch/intel/encoding_avx2.c")
    set_source_files_properties(${AWS_COMMON_AVX2_SRC} PROPERTIES COMPILE_FLAGS "${AVX2_CFLAGS}")
endif()

configure_file("${AWS_COMMON_DIR}/include/aws/common/config.h.in"
    "${CMAKE_CURRENT_BINARY_DIR}/include/aws/common/config.h" @ONLY)

list(APPEND AWS_SOURCES ${AWS_COMMON_SRC} ${AWS_COMMON_ARCH_SRC} ${AWS_COMMON_AVX2_SRC})

list(APPEND AWS_PUBLIC_INCLUDES
    "${AWS_COMMON_DIR}/include/"
    "${CMAKE_CURRENT_BINARY_DIR}/include"
)


# aws-checksums
file(GLOB AWS_CHECKSUMS_SRC
    "${AWS_CHECKSUMS_DIR}/source/*.c"
)

if(AWS_ARCH_INTEL AND AWS_HAVE_GCC_INLINE_ASM)
    file(GLOB AWS_CHECKSUMS_ARCH_SRC
            "${AWS_CHECKSUMS_DIR}/source/intel/asm/*.c"
    )
endif()

if (AWS_ARCH_ARM64)
    file(GLOB AWS_CHECKSUMS_ARCH_SRC
        "${AWS_CHECKSUMS_DIR}/source/arm/*.c"
    )
    set_source_files_properties("${AWS_CHECKSUMS_DIR}/source/arm/crc32c_arm.c" PROPERTIES COMPILE_FLAGS -march=armv8-a+crc)
endif()

list(APPEND AWS_SOURCES ${AWS_CHECKSUMS_SRC} ${AWS_CHECKSUMS_ARCH_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_CHECKSUMS_DIR}/include/")


# aws-c-io
file(GLOB AWS_IO_SRC
    "${AWS_IO_DIR}/source/*.c"
)

if (OS_LINUX)
    file(GLOB AWS_IO_OS_SRC
        "${AWS_IO_DIR}/source/linux/*.c"
        "${AWS_IO_DIR}/source/posix/*.c"
    )
elseif (OS_DARWIN)
    file(GLOB AWS_IO_OS_SRC
        "${AWS_IO_DIR}/source/bsd/*.c"
        "${AWS_IO_DIR}/source/posix/*.c"
        # Note darwin source files leads to compile errors, ignore them
        # "${AWS_IO_DIR}/source/darwin/*.c"
    )
endif()

set(AWS_IO_TLS_SRC)
if (USE_S2N)
    file(GLOB AWS_IO_TLS_SRC
            "${AWS_IO_DIR}/source/s2n/*.c"
    )
endif()

list(APPEND AWS_SOURCES ${AWS_IO_SRC} ${AWS_IO_OS_SRC} ${AWS_IO_TLS_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_IO_DIR}/include/")


# aws-s2n-tls
if (USE_S2N)
    file(GLOB AWS_S2N_TLS_SRC
        "${AWS_S2N_TLS_DIR}/crypto/*.c"
        "${AWS_S2N_TLS_DIR}/error/*.c"
        "${AWS_S2N_TLS_DIR}/stuffer/*.c"
        "${AWS_S2N_TLS_DIR}/pq-crypto/*.c"
        "${AWS_S2N_TLS_DIR}/pq-crypto/kyber_r3/*.c"
        "${AWS_S2N_TLS_DIR}/tls/*.c"
        "${AWS_S2N_TLS_DIR}/tls/extensions/*.c"
        "${AWS_S2N_TLS_DIR}/utils/*.c"
    )

    list(APPEND AWS_SOURCES ${AWS_S2N_TLS_SRC})

    list(APPEND AWS_PRIVATE_INCLUDES
        "${AWS_S2N_TLS_DIR}/"
        "${AWS_S2N_TLS_DIR}/api/"
    )
endif()


# aws-crt-cpp
file(GLOB AWS_CRT_SRC
    "${AWS_CRT_DIR}/source/*.cpp"
    "${AWS_CRT_DIR}/source/auth/*.cpp"
    "${AWS_CRT_DIR}/source/crypto/*.cpp"
    "${AWS_CRT_DIR}/source/endpoints/*.cpp"
    "${AWS_CRT_DIR}/source/http/*.cpp"
    "${AWS_CRT_DIR}/source/io/*.cpp"
)

list(APPEND AWS_SOURCES ${AWS_CRT_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_CRT_DIR}/include/")
configure_file("${AWS_CRT_DIR}/include/aws/crt/Config.h.in"
    ${CMAKE_CURRENT_BINARY_DIR}/include/aws/crt/Config.h @ONLY)


# aws-c-mqtt
file(GLOB AWS_MQTT_SRC
    "${AWS_MQTT_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_MQTT_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_MQTT_DIR}/include/")


# aws-c-http
file(GLOB AWS_HTTP_SRC
    "${AWS_HTTP_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_HTTP_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_HTTP_DIR}/include/")


# aws-c-compression
file(GLOB AWS_COMPRESSION_SRC
    "${AWS_COMPRESSION_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_COMPRESSION_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_COMPRESSION_DIR}/include/")


# aws-c-s3
file(GLOB AWS_S3_SRC
    "${AWS_S3_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_S3_SRC})
list(APPEND AWS_PRIVATE_INCLUDES "${AWS_S3_DIR}/include/")


# aws-c-sdkutils
file(GLOB AWS_SDKUTILS_SRC
    "${AWS_SDKUTILS_DIR}/source/*.c"
)

list(APPEND AWS_SOURCES ${AWS_SDKUTILS_SRC})
list(APPEND AWS_PUBLIC_INCLUDES "${AWS_SDKUTILS_DIR}/include/")


# Add library.
add_library(_aws ${AWS_SOURCES})

target_include_directories(_aws SYSTEM BEFORE PUBLIC ${AWS_PUBLIC_INCLUDES})
target_include_directories(_aws SYSTEM BEFORE PRIVATE ${AWS_PRIVATE_INCLUDES})
target_compile_definitions(_aws PUBLIC ${AWS_PUBLIC_COMPILE_DEFS})
target_compile_definitions(_aws PRIVATE ${AWS_PRIVATE_COMPILE_DEFS})
target_link_libraries(_aws PRIVATE ${AWS_PRIVATE_LIBS})
target_no_warning(_aws deprecated-declarations)
target_no_warning(_aws implicit-function-declaration)
target_no_warning(_aws int-conversion)
target_no_warning(_aws unused-function)

aws_set_thread_affinity_method(_aws)
aws_set_thread_name_method(_aws)

# The library is large - avoid bloat.
if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options (_aws PRIVATE -g0)
endif()

add_library(tiflash_contrib::aws_s3 ALIAS _aws)
