//
// Created by schrodinger on 7/8/21.
//

#ifndef CLICKHOUSE_DMFILEOPTION_H
#define CLICKHOUSE_DMFILEOPTION_H
#include <Common/config_version.h>
#include <IO/CompressionSettings.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>

namespace DB::DM
{
struct VersionOption
{
    bool                isSingleFileMode    = false;
    CompressionSettings compressionSettings = {};
};

struct LegacyOption : public VersionOption
{
};

struct FileV1Option : public VersionOption
{
    ChecksumAlgo                       checksumAlgo      = ChecksumAlgo::CRC64;
    size_t                             checksumBlockSize = TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE;
    std::map<std::string, std::string> extraInfo         = {
        {"creationCommitHash", TIFLASH_GIT_HASH},
        {"creationReleaseVersion", TIFLASH_RELEASE_VERSION},
        {"creationEdition", TIFLASH_EDITION},
    };
};
} // namespace DB::DM
#endif //CLICKHOUSE_DMFILEOPTION_H
