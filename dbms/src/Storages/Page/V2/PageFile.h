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

#include <IO/FileProvider/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/V2/PageEntries.h>
#include <Storages/Page/V2/VersionSet/PageEntriesEdit.h>
#include <Storages/Page/WriteBatchImpl.h>

#include <unordered_map>
#include <vector>


namespace DB::PS::V2
{
using PageIdAndEntry = PageIdU64AndEntry;
using PageIdAndEntries = PageIdU64AndEntries;
using PageMap = PageMapU64;

/// A light-weight object which can be created and copied cheaply.
/// Use createWriter()/createReader() to open write/read system file.
class PageFile : public Allocator<false>
{
public:
    /// Writer can NOT be used by multi threads.
    class Writer : private boost::noncopyable
    {
        friend class PageFile;

    public:
        Writer(PageFile &, bool sync_on_write, bool truncate_if_exists = true);
        ~Writer();

        [[nodiscard]] size_t write(
            DB::WriteBatch & wb,
            PageEntriesEdit & edit,
            const WriteLimiterPtr & write_limiter = nullptr,
            bool background = false);
        void tryCloseIdleFd(const Seconds & max_idle_time);

        const String & parentPath() const;
        PageFileIdAndLevel fileIdLevel() const;

        void hardlinkFrom(PageFile & linked_file, WriteBatch::SequenceID sid, PageEntriesEdit & edit);

    private:
        void closeFd();

    private:
        PageFile & page_file;
        bool sync_on_write;

        WritableFilePtr data_file;
        WritableFilePtr meta_file;

        Clock::time_point last_write_time; // use to free idle writers
    };

    /// Reader is safe to used by multi threads.
    class Reader
        : private boost::noncopyable
        , private Allocator<false>
    {
        friend class PageFile;

    public:
        explicit Reader(PageFile & page_file);
        ~Reader();

        /// Read pages from files.
        /// After return, the items in to_read could be reordered, but won't be removed or added.
        PageMap read(
            PageIdAndEntries & to_read,
            const ReadLimiterPtr & read_limiter = nullptr,
            bool background = false);

        struct FieldReadInfo
        {
            PageId page_id;
            PageEntry entry;
            std::vector<size_t> fields;

            FieldReadInfo(PageId id_, PageEntry entry_, std::vector<size_t> fields_)
                : page_id(id_)
                , entry(entry_)
                , fields(fields_)
            {}
        };
        using FieldReadInfos = std::vector<FieldReadInfo>;
        PageMap read(FieldReadInfos & to_read, const ReadLimiterPtr & read_limiter = nullptr);
        Page read(FieldReadInfo & to_read, const ReadLimiterPtr & read_limiter = nullptr);

        bool isIdle(const Seconds & max_idle_time);

    private:
        String data_file_path;

        RandomAccessFilePtr data_file;

        Clock::time_point last_read_time;
    };

    // PageFile with type "Checkpoint" is smaller than other types.
    // Then compare PageFile by theirs <FileID, Level>.
    struct Comparator
    {
        bool operator()(const PageFile & lhs, const PageFile & rhs) const
        {
            if (lhs.type != rhs.type)
            {
                // If any PageFile's type is checkpoint, it is smaller
                if (lhs.type == PageFile::Type::Checkpoint)
                    return true;
                else if (rhs.type == PageFile::Type::Checkpoint)
                    return false;
                // else fallback to later compare
            }
            return std::make_pair(lhs.file_id, lhs.level) < std::make_pair(rhs.file_id, rhs.level);
        }
    };

    class MetaMergingReader;
    using MetaMergingReaderPtr = std::shared_ptr<MetaMergingReader>;

    class MetaMergingReader : private boost::noncopyable
    {
    public:
        static MetaMergingReaderPtr createFrom(
            PageFile & page_file,
            size_t max_meta_offset,
            const ReadLimiterPtr & read_limiter = nullptr,
            bool background = false);

        static MetaMergingReaderPtr createFrom(
            PageFile & page_file,
            const ReadLimiterPtr & read_limiter = nullptr,
            bool background = false);

        explicit MetaMergingReader(PageFile & page_file_); // should only called by `createFrom`

        ~MetaMergingReader();

        enum class Status
        {
            Uninitialized = 0,
            Opened,
            Finished,
        };

    public:
        bool hasNext() const;

        void moveNext(PageFormat::Version * v = nullptr);

        PageEntriesEdit getEdits() { return std::move(curr_edit); }

        void setPageFileOffsets() { page_file.setFileAppendPos(meta_file_offset, data_file_offset); }

        String toString() const
        {
            return "MergingReader of " + page_file.toString() + ", sequence no: "
                + DB::toString(curr_write_batch_sequence) + ", meta offset: " + DB::toString(meta_file_offset)
                + ", data offset: " + DB::toString(data_file_offset);
        }

        WriteBatch::SequenceID writeBatchSequence() const { return curr_write_batch_sequence; }
        PageFileIdAndLevel fileIdLevel() const { return page_file.fileIdLevel(); }
        PageFile & belongingPageFile() { return page_file; }

        static bool compare(const MetaMergingReader & lhs, const MetaMergingReader & rhs)
        {
            if (lhs.page_file.getType() != rhs.page_file.getType())
            {
                // If one PageFile's type is checkpoint, it is smaller
                if (lhs.page_file.getType() == PageFile::Type::Checkpoint)
                    return true;
                else if (rhs.page_file.getType() == PageFile::Type::Checkpoint)
                    return false;
                // else fallback to later compare
            }
            if (lhs.curr_write_batch_sequence == rhs.curr_write_batch_sequence)
                return lhs.page_file.fileIdLevel() < rhs.page_file.fileIdLevel();
            return lhs.curr_write_batch_sequence < rhs.curr_write_batch_sequence;
        }

    private:
        void initialize(
            std::optional<size_t> max_meta_offset,
            const ReadLimiterPtr & read_limiter,
            bool background = false);

    private:
        PageFile & page_file;

        Status status = Status::Uninitialized;

        WriteBatch::SequenceID curr_write_batch_sequence = 0;
        PageEntriesEdit curr_edit;

        // The whole buffer and size of metadata, should be initlized in method `initlize()`.
        char * meta_buffer = nullptr;
        size_t meta_size = 0;

        // Current parsed offsets.
        size_t meta_file_offset = 0;
        size_t data_file_offset = 0;
    };

    class LinkingMetaAdapter;
    using LinkingMetaAdapterPtr = std::shared_ptr<LinkingMetaAdapter>;

    // This reader is used to link meta file.
    // After data linked, we need update meta with these step:
    // 1. update sequence id
    // 2. update crc
    class LinkingMetaAdapter : private boost::noncopyable
    {
    public:
        static LinkingMetaAdapterPtr createFrom(PageFile & page_file, const ReadLimiterPtr & read_limiter = nullptr);

        explicit LinkingMetaAdapter(PageFile & page_file_); // should only called by `createFrom`

        ~LinkingMetaAdapter();

        bool hasNext() const;

        bool linkToNewSequenceNext(WriteBatch::SequenceID sid, PageEntriesEdit & edit, UInt64 file_id, UInt64 level);

        std::pair<char *, size_t> getMetaInfo() { return {meta_buffer, meta_size}; };

    private:
        bool initialize(const ReadLimiterPtr & read_limiter);

    private:
        PageFile & page_file;
        char * meta_buffer = nullptr;
        size_t meta_size = 0;
        size_t meta_file_offset = 0;
        // TODO: Should also use a limited size buffer for reading. Will be done later in #2794
    };

    struct MergingPtrComparator
    {
        bool operator()(const MetaMergingReaderPtr & lhs, const MetaMergingReaderPtr & rhs) const
        {
            // priority_queue always pop the "biggest" elem
            return MetaMergingReader::compare(*rhs, *lhs);
        }
    };

public:
    enum class Type
    {
        Invalid = 0,
        Formal,
        Temp, // written by GC thread
        Legacy, // the data is obsoleted and has been removed, only meta left
        Checkpoint, // for recovery, only meta left
    };

    static String typeToString(Type type)
    {
        switch (type)
        {
        case Type::Invalid:
            return "Invalid";
        case Type::Formal:
            return "Formal";
        case Type::Temp:
            return "Temp";
        case Type::Legacy:
            return "Legacy";
        case Type::Checkpoint:
            return "Checkpoint";
        default:
            throw Exception(fmt::format("Unexpected PageFile::Type: {}", static_cast<int>(type)));
        }
    }

    /// Create an empty page file.
    PageFile() = default;
    /// Recover a page file from disk.
    static std::pair<PageFile, Type> recover(
        const String & parent_path,
        const FileProviderPtr & file_provider_,
        const String & page_file_name,
        LoggerPtr log);
    /// Create a new page file.
    static PageFile newPageFile(
        PageFileId file_id,
        UInt32 level,
        const String & parent_path,
        const FileProviderPtr & file_provider_,
        Type type,
        LoggerPtr log);
    /// Open an existing page file for read.
    static PageFile openPageFileForRead(
        PageFileId file_id,
        UInt32 level,
        const String & parent_path,
        const FileProviderPtr & file_provider_,
        Type type,
        LoggerPtr log);
    /// If page file is exist.
    static bool isPageFileExist(
        PageFileIdAndLevel file_id,
        const String & parent_path,
        const FileProviderPtr & file_provider_,
        Type type,
        LoggerPtr log);

    /// Rename this page file into formal style.
    void setFormal();
    /// Rename this page file into legacy style and remove data.
    size_t setLegacy();
    /// Rename this page file into checkpoint style.
    size_t setCheckpoint();
    /// Destroy underlying system files.
    void destroy() const;

    /// Return a writer bound with this PageFile object.
    /// Note that the user MUST keep the PageFile object around before this writer being freed.
    /// And the meta_file_pos, data_file_pos should be properly set before creating writer.
    std::unique_ptr<Writer> createWriter(bool sync_on_write, bool truncate_if_exists)
    {
        return std::make_unique<Writer>(*this, sync_on_write, truncate_if_exists);
    }
    /// Return a reader for this file.
    /// The PageFile object can be released any time.
    std::shared_ptr<Reader> createReader()
    {
        if (unlikely(type != Type::Formal))
            throw Exception(
                "Try to create reader for PageFile_" + DB::toString(file_id) + "_" + DB::toString(level)
                    + " of illegal type: " + typeToString(type),
                ErrorCodes::LOGICAL_ERROR);

        return std::make_shared<Reader>(*this);
    }

    UInt64 getFileId() const { return file_id; }
    UInt32 getLevel() const { return level; }
    PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }
    bool isValid() const { return file_id; }
    bool isExist() const;
    Type getType() const { return type; }

    void setFileAppendPos(size_t meta_pos, size_t data_pos);
    UInt64 getDataFileAppendPos() const { return data_file_pos; }
    UInt64 getMetaFileAppendPos() const { return meta_file_pos; }

    /// Get disk usage
    // Total size, data && meta.
    UInt64 getDiskSize() const;
    UInt64 getDataFileSize() const;
    UInt64 getMetaFileSize() const;

    String parentPath() const { return parent_path; }
    String folderPath() const;

    [[nodiscard]] bool linkFrom(PageFile & page_file, WriteBatch::SequenceID sid, PageEntriesEdit & edit);

    void createEncryptionInfo() const
    {
        file_provider->createEncryptionInfo(dataEncryptionPath());
        file_provider->createEncryptionInfo(metaEncryptionPath());
    }

    void deleteEncryptionInfo() const
    {
        file_provider->deleteEncryptionInfo(dataEncryptionPath());
        file_provider->deleteEncryptionInfo(metaEncryptionPath());
    }

    // Check whether this PageFile is resuable for writing after restart
    bool reusableForWrite() const
    {
        // If the file is created by gc thread (level != 0)
        // or ready for gc, then it is not resuable.
        if (level != 0 || type != PageFile::Type::Formal)
            return false;
        // Encryption can be turned on / turned off for existing cluster, we should take care of it when trying to reuse PageFile.
        auto file_encrypted = file_provider->isFileEncrypted(dataEncryptionPath());
        auto encryption_enabled = file_provider->isEncryptionEnabled();
        return (file_encrypted && encryption_enabled) || (!file_encrypted && !encryption_enabled);
    }

    String toString() const
    {
        return "PageFile_" + DB::toString(file_id) + "_" + DB::toString(level) + ", type: " + typeToString(type);
    }

private:
    /// Create a new page file.
    PageFile(
        PageFileId file_id_,
        UInt32 level_,
        const String & parent_path,
        const FileProviderPtr & file_provider_,
        Type type_,
        bool is_create,
        LoggerPtr log);

    String dataPath() const { return folderPath() + "/page"; }
    String metaPath() const { return folderPath() + "/meta"; }

    EncryptionPath dataEncryptionPath() const { return EncryptionPath(dataPath(), ""); }
    EncryptionPath metaEncryptionPath() const { return EncryptionPath(metaPath(), ""); }

    constexpr static const char * folder_prefix_formal = "page";
    constexpr static const char * folder_prefix_temp = ".temp.page";
    constexpr static const char * folder_prefix_legacy = "legacy.page";
    constexpr static const char * folder_prefix_checkpoint = "checkpoint.page";

    size_t removeDataIfExists() const;

private:
    UInt64 file_id = 0; // Valid id start from 1.
    UInt32 level = 0; // 0: normal, >= 1: generated by GC.
    Type type = Type::Formal;
    String parent_path{}; // The parent folder of this page file.

    FileProviderPtr file_provider;

    // The append pos.
    UInt64 data_file_pos = 0;
    UInt64 meta_file_pos = 0;

    LoggerPtr log = nullptr;
};
using PageFileSet = std::set<PageFile, PageFile::Comparator>;

void removePageFilesIf(PageFileSet & page_files, const std::function<bool(const PageFile &)> & pred);

} // namespace DB::PS::V2
