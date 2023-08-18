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

#include <Common/CurrentMetrics.h>
#include <Encryption/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/V1/Page.h>
#include <Storages/Page/V1/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/V1/WriteBatch.h>

#include <unordered_map>
#include <vector>

namespace Poco
{
class Logger;
} // namespace Poco
namespace CurrentMetrics
{
extern const Metric OpenFileForWrite;
extern const Metric OpenFileForRead;
} // namespace CurrentMetrics

namespace DB::PS::V1
{
/// A light-weight object which can be created and copied cheaply.
/// Use createWriter()/createReader() to open write/read system file.
class PageFile : public Allocator<false>
{
public:
    using Version = UInt32;

    static const Version CURRENT_VERSION;

    /// Writer can NOT be used by multi threads.
    class Writer : private boost::noncopyable
    {
        friend class PageFile;

    public:
        Writer(PageFile &, bool sync_on_write, bool create_new_file = true);
        ~Writer();

        void write(const WriteBatch & wb, PageEntriesEdit & edit);

    private:
        PageFile & page_file;
        bool sync_on_write;

        WritableFilePtr data_file;
        WritableFilePtr meta_file;
    };

    /// Reader is safe to used by multi threads.
    class Reader : private boost::noncopyable
        , private Allocator<false>
    {
        friend class PageFile;

    public:
        explicit Reader(PageFile & page_file);
        ~Reader();

        /// Read pages from files.
        /// After return, the items in to_read could be reordered, but won't be removed or added.
        PageMap read(PageIdAndEntries & to_read);

        void read(PageIdAndEntries & to_read, const PageHandler & handler);

    private:
        String data_file_path;

        RandomAccessFilePtr data_file;

        CurrentMetrics::Increment fd_increment{CurrentMetrics::OpenFileForRead};
    };

    struct Comparator
    {
        bool operator()(const PageFile & lhs, const PageFile & rhs) const
        {
            return std::make_pair(lhs.file_id, lhs.level) < std::make_pair(rhs.file_id, rhs.level);
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
            throw Exception("Unexpected PageFile::Type: " + DB::toString((int)type));
        }
    }

    /// Create an empty page file.
    PageFile() = default;
    /// Recover a page file from disk.
    static std::pair<PageFile, Type> recover(const String & parent_path, const FileProviderPtr & file_provider, const String & page_file_name, Poco::Logger * log);
    /// Create a new page file.
    static PageFile newPageFile(PageFileId file_id, UInt32 level, const String & parent_path, const FileProviderPtr & file_provider, Type type, Poco::Logger * log);
    /// Open an existing page file for read.
    static PageFile openPageFileForRead(PageFileId file_id, UInt32 level, const String & parent_path, const FileProviderPtr & file_provider, Type type, Poco::Logger * log);

    /// Get pages' metadata by this method. Will also update file pos.
    /// Call this method after a page file recovered.
    /// if check_page_map_complete is true, do del or ref on non-exist page will throw exception.
    void readAndSetPageMetas(PageEntriesEdit & edit);

    /// Rename this page file into formal style.
    void setFormal();
    /// Rename this page file into legacy style and remove data.
    void setLegacy();
    /// Rename this page file into checkpoint style.
    void setCheckpoint();
    /// Destroy underlying system files.
    void destroy() const;

    /// Return a writer bound with this PageFile object.
    /// Note that the user MUST keep the PageFile object around before this writer being freed.
    std::unique_ptr<Writer> createWriter(bool sync_on_write, bool create_new_file) { return std::make_unique<Writer>(*this, sync_on_write, create_new_file); }
    /// Return a reader for this file.
    /// The PageFile object can be released any time.
    std::shared_ptr<Reader> createReader()
    {
        if (unlikely(type != Type::Formal))
            throw Exception("Try to create reader for PageFile_" + DB::toString(file_id) + "_" + DB::toString(level)
                                + " of illegal type: " + typeToString(type),
                            ErrorCodes::LOGICAL_ERROR);

        return std::make_shared<Reader>(*this);
    }

    UInt64 getFileId() const { return file_id; }
    UInt32 getLevel() const { return level; }
    PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }
    bool isValid() const { return file_id; }
    UInt64 getDataFileAppendPos() const { return data_file_pos; }
    UInt64 getDataFileSize() const;
    bool isExist() const;
    void removeDataIfExists() const;
    Type getType() const { return type; }

    String folderPath() const;

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

    // Encryption can be turned on / turned off for existing cluster, we should take care of it when trying to reuse PageFile.
    bool reusableForWrite() const
    {
        auto file_encrypted = file_provider->isFileEncrypted(dataEncryptionPath());
        auto encryption_enabled = file_provider->isEncryptionEnabled();
        return (file_encrypted && encryption_enabled) || (!file_encrypted && !encryption_enabled);
    }

private:
    /// Create a new page file.
    PageFile(PageFileId file_id_, UInt32 level_, const String & parent_path, const FileProviderPtr & file_provider_, Type type_, bool is_create, Poco::Logger * log);

    String dataPath() const { return folderPath() + "/page"; }
    String metaPath() const { return folderPath() + "/meta"; }

    EncryptionPath dataEncryptionPath() const
    {
        return EncryptionPath(dataPath(), "");
    }
    EncryptionPath metaEncryptionPath() const
    {
        return EncryptionPath(metaPath(), "");
    }

    constexpr static const char * folder_prefix_formal = "page";
    constexpr static const char * folder_prefix_temp = ".temp.page";
    constexpr static const char * folder_prefix_legacy = "legacy.page";
    constexpr static const char * folder_prefix_checkpoint = "checkpoint.page";

private:
    UInt64 file_id = 0; // Valid id start from 1.
    UInt32 level = 0; // 0: normal, >= 1: generated by GC.
    Type type = Type::Formal;
    String parent_path{}; // The parent folder of this page file.

    FileProviderPtr file_provider;

    // The append pos.
    UInt64 data_file_pos = 0;
    UInt64 meta_file_pos = 0;

    Poco::Logger * log = nullptr;
};

} // namespace DB::PS::V1
