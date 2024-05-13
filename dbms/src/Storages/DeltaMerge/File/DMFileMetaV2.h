// Copyright 2024 PingCAP, Inc.
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

#include <Storages/DeltaMerge/File/DMFileMeta.h>

namespace DB::DM
{

class DMFileMetaV2 : public DMFileMeta
{
public:
    DMFileMetaV2(
        UInt64 file_id_,
        const String & parent_path_,
        DMFileStatus status_,
        UInt64 small_file_size_threshold_,
        UInt64 merged_file_max_size_,
        KeyspaceID keyspace_id_,
        DMConfigurationOpt configuration_,
        DMFileFormat::Version version_)
        : DMFileMeta(file_id_, parent_path_, status_, keyspace_id_, configuration_, version_)
        , small_file_size_threshold(small_file_size_threshold_)
        , merged_file_max_size(merged_file_max_size_)
    {}

    ~DMFileMetaV2() override = default;

    String mergedPath(UInt32 number) const { return subFilePath(mergedFilename(number)); }

    /* New metadata file format:
     * |Pack Stats|Pack Properties|Column Stats|Pack Stats Handle|Pack Properties Handle|Column Stats Handle|Meta Block Handle Count|DMFile Version|Checksum|Footer|
     * |----------------------------------------Checksum include-----------------------------------------------------------------------------------|
     * `Footer` is saved at the end of the file, with fixed length, it contains checksum algorithm and checksum frame length.
     * First, read `Footer` and `Checksum`, and check data integrity.
     * Second, parse handle and parse corresponding data.
     * `PackStatsHandle`, `PackPropertiesHandle` and `ColumnStatsHandle` are offset and size of `PackStats`, `PackProperties` and `ColumnStats`.
     */
    // Meta data is small and 64KB is enough.
    static constexpr size_t meta_buffer_size = 64 * 1024;

    struct MergedFile
    {
        UInt64 number = 0;
        UInt64 size = 0;
    };

    struct MergedFileWriter
    {
        MergedFile file_info;
        WriteBufferFromWritableFilePtr buffer;
    };
    PaddedPODArray<MergedFile> merged_files;
    // Filename -> MergedSubFileInfo
    std::unordered_map<String, MergedSubFileInfo> merged_sub_file_infos;

    void finalizeSmallFiles(
        MergedFileWriter & writer,
        FileProviderPtr & file_provider,
        WriteLimiterPtr & write_limiter);
    // check if the size of merged file is larger then the threshold. If so, create a new merged file.
    void checkMergedFile(MergedFileWriter & writer, FileProviderPtr & file_provider, WriteLimiterPtr & write_limiter);

    void finalize(WriteBuffer & buffer, const FileProviderPtr & file_provider, const WriteLimiterPtr & write_limiter)
        override;
    void read(const FileProviderPtr & file_provider, const DMFileMeta::ReadMode & read_meta_mode) override;
    static String metaFileName() { return "meta"; }
    String metaPath() const override { return subFilePath(metaFileName()); }
    EncryptionPath encryptionMetaPath() const override;

    UInt64 getReadFileSize(ColId col_id, const String & filename) const override;
    EncryptionPath encryptionMergedPath(UInt32 number) const;
    static String mergedFilename(UInt32 number) { return fmt::format("{}.merged", number); }

    UInt64 small_file_size_threshold;
    UInt64 merged_file_max_size;

private:
    UInt64 getMergedFileSizeOfColumn(const MergedSubFileInfo & file_info) const;

    // finalize
    BlockHandle writeSLPackStatToBuffer(WriteBuffer & buffer);
    BlockHandle writeSLPackPropertyToBuffer(WriteBuffer & buffer) const;
    BlockHandle writeColumnStatToBuffer(WriteBuffer & buffer);
    BlockHandle writeExtendColumnStatToBuffer(WriteBuffer & buffer);
    BlockHandle writeMergedSubFilePosotionsToBuffer(WriteBuffer & buffer);

    // read
    void parse(std::string_view buffer);
    void parseColumnStat(std::string_view buffer);
    void parseExtendColumnStat(std::string_view buffer);
    void parseMergedSubFilePos(std::string_view buffer);
    void parsePackProperty(std::string_view buffer);
    void parsePackStat(std::string_view buffer);
};

} // namespace DB::DM
