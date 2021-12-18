#pragma once

#include <Encryption/FileProvider.h>
#include <IO/WriteHelpers.h>
#include <Storages/FormatVersion.h>
#include <Storages/Page/Page.h>
#include <Storages/Page/PageDefines.h>
#include <Storages/Page/VersionSet/PageEntriesVersionSet.h>
#include <Storages/Page/WriteBatch.h>

#include <unordered_map>
#include <vector>

namespace Poco
{
class Logger;
} // namespace Poco

namespace DB
{

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
        Writer(PageFile &, bool sync_on_write, bool create_new_file = true);
        ~Writer();

        [[nodiscard]] size_t write(WriteBatch & wb, PageEntriesEdit & edit, const RateLimiterPtr & rate_limiter = nullptr);
        void                 tryCloseIdleFd(const Seconds & max_idle_time);

        const String &     parentPath() const;
        PageFileIdAndLevel fileIdLevel() const;

    private:
        void closeFd();

    private:
        PageFile & page_file;
        bool       sync_on_write;

        WritableFilePtr data_file;
        WritableFilePtr meta_file;

        Clock::time_point last_write_time; // use to free idle writers
    };

    /// Reader is safe to used by multi threads.
    class Reader : private boost::noncopyable, private Allocator<false>
    {
        friend class PageFile;

    public:
        explicit Reader(PageFile & page_file);
        ~Reader();

        /// Read pages from files.
        /// After return, the items in to_read could be reordered, but won't be removed or added.
        PageMap read(PageIdAndEntries & to_read);

        void read(PageIdAndEntries & to_read, const PageHandler & handler);

        struct FieldReadInfo
        {
            PageId              page_id;
            PageEntry           entry;
            std::vector<size_t> fields;

            FieldReadInfo(PageId id_, PageEntry entry_, std::vector<size_t> fields_) : page_id(id_), entry(entry_), fields(fields_) {}
        };
        using FieldReadInfos = std::vector<FieldReadInfo>;
        PageMap read(FieldReadInfos & to_read);

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
        static MetaMergingReaderPtr createFrom(PageFile & page_file, size_t max_meta_offset);
        static MetaMergingReaderPtr createFrom(PageFile & page_file);

        MetaMergingReader(PageFile & page_file_); // should only called by `createFrom`

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
            return "MergingReader of " + page_file.toString() + ", sequence no: " + DB::toString(curr_write_batch_sequence)
                + ", meta offset: " + DB::toString(meta_file_offset) + ", data offset: " + DB::toString(data_file_offset);
        }

        WriteBatch::SequenceID writeBatchSequence() const { return curr_write_batch_sequence; }
        PageFileIdAndLevel     fileIdLevel() const { return page_file.fileIdLevel(); }
        PageFile &             belongingPageFile() { return page_file; }

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

        void initialize(std::optional<size_t> max_meta_offset);

    private:
        PageFile & page_file;

        Status status = Status::Uninitialized;

        WriteBatch::SequenceID curr_write_batch_sequence = 0;
        PageEntriesEdit        curr_edit;

        // The whole buffer and size of metadata, should be initlized in method `initlize()`.
        char * meta_buffer = nullptr;
        size_t meta_size   = 0;

        // Current parsed offsets.
        size_t meta_file_offset = 0;
        size_t data_file_offset = 0;
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
        Temp,       // written by GC thread
        Legacy,     // the data is obsoleted and has been removed, only meta left
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
    static std::pair<PageFile, Type>
    recover(const String & parent_path, const FileProviderPtr & file_provider_, const String & page_file_name, Poco::Logger * log);
    /// Create a new page file.
    static PageFile newPageFile(PageFileId              file_id,
                                UInt32                  level,
                                const String &          parent_path,
                                const FileProviderPtr & file_provider_,
                                Type                    type,
                                Poco::Logger *          log);
    /// Open an existing page file for read.
    static PageFile openPageFileForRead(PageFileId              file_id,
                                        UInt32                  level,
                                        const String &          parent_path,
                                        const FileProviderPtr & file_provider_,
                                        Type                    type,
                                        Poco::Logger *          log);
    /// If page file is exist.
    static bool isPageFileExist(
        PageFileIdAndLevel file_id, const String & parent_path, const FileProviderPtr & file_provider_, Type type, Poco::Logger * log);

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
            throw Exception("Try to create reader for PageFile_" + DB::toString(file_id) + "_" + DB::toString(level)
                                + " of illegal type: " + typeToString(type),
                            ErrorCodes::LOGICAL_ERROR);

        return std::make_shared<Reader>(*this);
    }

    UInt64             getFileId() const { return file_id; }
    UInt32             getLevel() const { return level; }
    PageFileIdAndLevel fileIdLevel() const { return std::make_pair(file_id, level); }
    bool               isValid() const { return file_id; }
    bool               isExist() const;
    Type               getType() const { return type; }

    void   setFileAppendPos(size_t meta_pos, size_t data_pos);
    UInt64 getDataFileAppendPos() const { return data_file_pos; }
    UInt64 getMetaFileAppendPos() const { return meta_file_pos; }

    /// Get disk usage
    // Total size, data && meta.
    UInt64 getDiskSize() const;
    UInt64 getDataFileSize() const;
    UInt64 getMetaFileSize() const;

    String parentPath() const { return parent_path; }
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

    // Check whether this PageFile is resuable for writing after restart
    bool reusableForWrite() const
    {
        // If the file is created by gc thread (level != 0)
        // or ready for gc, then it is not resuable.
        if (level != 0 || type != PageFile::Type::Formal)
            return false;
        // Encryption can be turned on / turned off for existing cluster, we should take care of it when trying to reuse PageFile.
        auto file_encrypted     = file_provider->isFileEncrypted(dataEncryptionPath());
        auto encryption_enabled = file_provider->isEncryptionEnabled();
        return (file_encrypted && encryption_enabled) || (!file_encrypted && !encryption_enabled);
    }

    String toString() const { return "PageFile_" + DB::toString(file_id) + "_" + DB::toString(level) + ", type: " + typeToString(type); }

private:
    /// Create a new page file.
    PageFile(PageFileId              file_id_,
             UInt32                  level_,
             const String &          parent_path,
             const FileProviderPtr & file_provider_,
             Type                    type_,
             bool                    is_create,
             Poco::Logger *          log);

    String dataPath() const { return folderPath() + "/page"; }
    String metaPath() const { return folderPath() + "/meta"; }

    EncryptionPath dataEncryptionPath() const { return EncryptionPath(dataPath(), ""); }
    EncryptionPath metaEncryptionPath() const { return EncryptionPath(metaPath(), ""); }

    constexpr static const char * folder_prefix_formal     = "page";
    constexpr static const char * folder_prefix_temp       = ".temp.page";
    constexpr static const char * folder_prefix_legacy     = "legacy.page";
    constexpr static const char * folder_prefix_checkpoint = "checkpoint.page";

    size_t removeDataIfExists() const;

private:
    UInt64 file_id = 0; // Valid id start from 1.
    UInt32 level   = 0; // 0: normal, >= 1: generated by GC.
    Type   type    = Type::Formal;
    String parent_path{}; // The parent folder of this page file.

    FileProviderPtr file_provider;

    // The append pos.
    UInt64 data_file_pos = 0;
    UInt64 meta_file_pos = 0;

    Poco::Logger * log = nullptr;
};
using PageFileSet = std::set<PageFile, PageFile::Comparator>;

void removePageFilesIf(PageFileSet & page_files, const std::function<bool(const PageFile &)> & pred);

} // namespace DB
