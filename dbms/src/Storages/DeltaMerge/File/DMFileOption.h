//
// Created by schrodinger on 7/8/21.
//

#ifndef CLICKHOUSE_DMFILEOPTION_H
#define CLICKHOUSE_DMFILEOPTION_H
#include <Common/config_version.h>
#include <IO/CompressionSettings.h>
#include <Storages/DeltaMerge/File/Checksum/ChecksumBuffer.h>
#include <Storages/DeltaMerge/File/DMFileVersion.h>

namespace DB::DM
{
struct DMFileOption
{
    bool                isSingleFileMode    = false;
    CompressionSettings compressionSettings = {};
    FileVersion         fileVersion         = CURRENT_FILE_VERSION;
};

struct LegacyOption : public DMFileOption
{
};

struct FileV1Option : public DMFileOption
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
