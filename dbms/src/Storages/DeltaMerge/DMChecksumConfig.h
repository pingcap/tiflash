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

#pragma once
#include <Common/TiFlashBuildInfo.h>
#include <Common/TiFlashException.h>
#include <IO/Checksum/ChecksumBuffer.h>
#include <Interpreters/Context_fwd.h>

#include <map>
#include <string>

namespace DB::DM
{
class DMChecksumConfig
{
public:
    explicit DMChecksumConfig(std::istream & input);

    explicit DMChecksumConfig(
        std::map<std::string, std::string> embedded_checksum_ = {},
        size_t checksum_frame_length_ = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE,
        DB::ChecksumAlgo checksum_algorithm_ = DB::ChecksumAlgo::XXH3,
        std::map<std::string, std::string> debug_info_
        = {{"creation_commit_hash", TiFlashBuildInfo::getGitHash()},
           {"creation_edition", TiFlashBuildInfo::getEdition()},
           {"creation_version", TiFlashBuildInfo::getVersion()},
           {"creation_release_version", TiFlashBuildInfo::getReleaseVersion()},
           {"creation_build_time", TiFlashBuildInfo::getUTCBuildTime()}})
        : checksum_frame_length(checksum_frame_length_)
        , checksum_algorithm(checksum_algorithm_)
        , embedded_checksum(std::move(embedded_checksum_))
        , debug_info(std::move(debug_info_))
    {}

    friend std::ostream & operator<<(std::ostream &, const DMChecksumConfig &);

    [[nodiscard]] size_t getChecksumFrameLength() const { return checksum_frame_length; }
    [[nodiscard]] size_t getChecksumHeaderLength() const
    {
        switch (checksum_algorithm)
        {
        case DB::ChecksumAlgo::None:
            return sizeof(DB::ChecksumFrame<DB::Digest::None>);
        case DB::ChecksumAlgo::CRC32:
            return sizeof(DB::ChecksumFrame<DB::Digest::CRC32>);
        case DB::ChecksumAlgo::CRC64:
            return sizeof(DB::ChecksumFrame<DB::Digest::CRC64>);
        case DB::ChecksumAlgo::City128:
            return sizeof(DB::ChecksumFrame<DB::Digest::City128>);
        case DB::ChecksumAlgo::XXH3:
            return sizeof(DB::ChecksumFrame<DB::Digest::XXH3>);
        }
        throw TiFlashException("unrecognized checksum algorithm", Errors::Checksum::Internal);
    }
    [[nodiscard]] DB::ChecksumAlgo getChecksumAlgorithm() const { return checksum_algorithm; }
    [[nodiscard]] const std::map<std::string, std::string> & getEmbeddedChecksum() const { return embedded_checksum; }
    [[nodiscard]] const std::map<std::string, std::string> & getDebugInfo() const { return debug_info; }

    void addChecksum(std::string name, std::string value) { embedded_checksum[std::move(name)] = std::move(value); }

    [[nodiscard]] DB::UnifiedDigestBaseBox createUnifiedDigest() const
    {
        switch (checksum_algorithm)
        {
        case DB::ChecksumAlgo::None:
            return std::make_unique<DB::UnifiedDigest<DB::Digest::None>>();
        case DB::ChecksumAlgo::CRC32:
            return std::make_unique<DB::UnifiedDigest<DB::Digest::CRC32>>();
        case DB::ChecksumAlgo::CRC64:
            return std::make_unique<DB::UnifiedDigest<DB::Digest::CRC64>>();
        case DB::ChecksumAlgo::City128:
            return std::make_unique<DB::UnifiedDigest<DB::Digest::City128>>();
        case DB::ChecksumAlgo::XXH3:
            return std::make_unique<DB::UnifiedDigest<DB::Digest::XXH3>>();
        default:
            throw TiFlashException("unrecognized checksum algorithm", Errors::Checksum::Internal);
        }
    }

    [[maybe_unused]] static std::optional<DMChecksumConfig> fromDBContext(const DB::Context & context);

private:
    size_t checksum_frame_length; ///< the length of checksum frame
    DB::ChecksumAlgo checksum_algorithm; ///< the algorithm of checksum
    std::map<std::string, std::string> embedded_checksum; ///< special checksums for meta files
    std::map<std::string, std::string> debug_info; ///< debugging information

    explicit DMChecksumConfig(const DB::Context & context);
};


std::ostream & operator<<(std::ostream & output, const DMChecksumConfig & config);

using DMConfigurationOpt = std::optional<DMChecksumConfig>;
} // namespace DB::DM
