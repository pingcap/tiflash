// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/TiFlashBuildInfo.h>
#include <Common/config.h>
#include <Common/config_version.h>
#include <Storages/DeltaMerge/Index/VectorIndexHNSW/SIMDFeatures.h>
#include <TiDB/Decode/Vector.h>
#include <common/config_common.h>
#include <common/logger_useful.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <openssl/opensslconf.h>
#include <openssl/opensslv.h>

#include <vector>

namespace TiFlashBuildInfo
{
String getName()
{
    return TIFLASH_NAME;
}
String getVersion()
{
    return TIFLASH_VERSION;
}
String getReleaseVersion()
{
    return TIFLASH_RELEASE_VERSION;
}
String getEdition()
{
    return TIFLASH_EDITION;
}
String getGitHash()
{
    return TIFLASH_GIT_HASH;
}
String getGitBranch()
{
    return TIFLASH_GIT_BRANCH;
}
String getUTCBuildTime()
{
    return TIFLASH_UTC_BUILD_TIME;
}
UInt32 getMajorVersion()
{
    return TIFLASH_VERSION_MAJOR;
}
UInt32 getMinorVersion()
{
    return TIFLASH_VERSION_MINOR;
}
UInt32 getPatchVersion()
{
    return TIFLASH_VERSION_PATCH;
}
// clang-format off
String getEnabledFeatures()
{
    std::vector<String> features
    {
// allocator
#if USE_JEMALLOC
            "jemalloc",
#elif USE_MIMALLOC
            "mimalloc",
#endif

// sm4
#if USE_GM_SSL
            "sm4(GmSSL)",
#elif OPENSSL_VERSION_NUMBER >= 0x1010100fL && !defined(OPENSSL_NO_SM4)
            "sm4(OpenSSL)",
#endif

// mem-profiling
#if USE_JEMALLOC_PROF
            "mem-profiling",
#endif

// failpoints
#if ENABLE_FAILPOINTS
            "failpoints",
#endif

// SIMD related
#ifdef TIFLASH_ENABLE_AVX_SUPPORT
            "avx2",
#endif
#ifdef TIFLASH_ENABLE_AVX512_SUPPORT
            "avx512",
#endif
#ifdef TIFLASH_ENABLE_ASIMD_SUPPORT
            "asimd",
#endif
#ifdef TIFLASH_ENABLE_SVE_SUPPORT
            "sve",
#endif

// Unwind related
#if USE_UNWIND
#if USE_LLVM_LIBUNWIND
            "llvm-unwind",
#else
            "unwind",
#endif
#endif

// THINLTO
#if ENABLE_THINLTO
            "thinlto",
#endif

// Profile instrumentation
#if ENABLE_LLVM_PROFILE_INSTR
            "profile-instr",
#endif

// PGO
#if ENABLE_LLVM_PGO_USE_SAMPLE
            "pgo-sample",
#elif ENABLE_LLVM_PGO
            "pgo-instr",
#endif

// FDO
#if USE_LLVM_FDO
            "fdo",
#endif
    };
    {
        auto f = DB::DM::VectorIndexHNSWSIMDFeatures::get();
        for (const auto & feature : f)
            features.push_back(feature);
    }
    {
        auto f = DB::VectorDistanceSIMDFeatures::get();
        for (const auto & feature : f)
            features.push_back(feature);
    }

    return fmt::format("{}", fmt::join(features.begin(), features.end(), " "));
}
// clang-format on
String getProfile()
{
    return TIFLASH_PROFILE;
}

String getCompilerVersion()
{
    return fmt::format(
        "{} {}",
        // TIFLASH_CXX_COMPILER is some strings like "/tiflash-env-17/sysroot/bin/clang++",
        // use `LogFmtDetails::getFileNameOffset` to get the compiler name
        &TIFLASH_CXX_COMPILER[LogFmtDetails::getFileNameOffset(TIFLASH_CXX_COMPILER)],
        TIFLASH_CXX_COMPILER_VERSION);
}

void outputDetail(std::ostream & os)
{
    os << getName() << std::endl
       << "Release Version: " << getReleaseVersion() << std::endl
       << "Edition:         " << getEdition() << std::endl
       << "Git Commit Hash: " << getGitHash() << std::endl
       << "Git Branch:      " << getGitBranch() << std::endl
       << "UTC Build Time:  " << getUTCBuildTime() << std::endl
       << "Enable Features: " << getEnabledFeatures() << std::endl
       << "Profile:         " << getProfile() << std::endl
       << "Compiler:        " << getCompilerVersion() << std::endl;
}
} // namespace TiFlashBuildInfo
