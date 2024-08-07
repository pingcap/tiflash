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

#include <Common/config.h>
#include <Common/config_version.h>
#include <VectorFeatures/DistanceSIMDFeatures.h>
#include <VectorFeatures/SIMDFeatures.h>
#include <common/config_common.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <openssl/opensslconf.h>
#include <openssl/opensslv.h>

#include <ostream>
#include <string>
#include <vector>

namespace TiFlashBuildInfo
{
std::string getName()
{
    return TIFLASH_NAME;
}
std::string getVersion()
{
    return TIFLASH_VERSION;
}
std::string getReleaseVersion()
{
    return TIFLASH_RELEASE_VERSION;
}
std::string getEdition()
{
    return TIFLASH_EDITION;
}
std::string getGitHash()
{
    return TIFLASH_GIT_HASH;
}
std::string getGitBranch()
{
    return TIFLASH_GIT_BRANCH;
}
std::string getUTCBuildTime()
{
    return TIFLASH_UTC_BUILD_TIME;
}
// clang-format off
std::string getEnabledFeatures()
{
    std::vector<std::string> features
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
std::string getProfile()
{
    return TIFLASH_PROFILE;
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
       << "Profile:         " << getProfile() << std::endl;
}
} // namespace TiFlashBuildInfo
