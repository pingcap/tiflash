#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/randomSeed.h>
#include <common/logger_useful.h>

#include <IO/WriteHelpers.h>

#ifndef __APPLE__
#include <fcntl.h>
#endif

#include <IO/WriteBufferFromFile.h>
#include <Poco/File.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>

#include <Storages/Page/PageFile.h>

namespace ProfileEvents
{
extern const Event FileOpen;
extern const Event FileOpenFailed;
extern const Event Seek;
extern const Event PSMWritePages;
extern const Event PSMWriteCalls;
extern const Event PSMWriteIOCalls;
extern const Event PSMWriteBytes;
extern const Event PSMReadPages;
extern const Event PSMReadCalls;
extern const Event PSMReadIOCalls;
extern const Event PSMReadBytes;
extern const Event PSMWriteFailed;
extern const Event PSMReadFailed;
} // namespace ProfileEvents

namespace CurrentMetrics
{
extern const Metric Write;
extern const Metric Read;
} // namespace CurrentMetrics

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT_VERSION;
extern const int CHECKSUM_DOESNT_MATCH;
extern const int FILE_DOESNT_EXIST;
extern const int CANNOT_OPEN_FILE;
extern const int CANNOT_FSYNC;
extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
extern const int CANNOT_SEEK_THROUGH_FILE;
extern const int PAGE_SIZE_NOT_MATCH;
extern const int LOGICAL_ERROR;
extern const int ILLFORMED_PAGE_NAME;
extern const int FILE_SIZE_NOT_MATCH;
} // namespace ErrorCodes

// =========================================================
// Helper functions
// =========================================================

static constexpr bool PAGE_CHECKSUM_ON_READ = true;

#ifndef O_DIRECT
#define O_DIRECT 00040000
#endif

template <bool read, bool must_exist = true>
int openFile(const std::string & path)
{
    ProfileEvents::increment(ProfileEvents::FileOpen);

    int flags;
    if constexpr (read)
    {
        flags = O_RDONLY;
    }
    else
    {
        flags = O_WRONLY | O_CREAT;
    }

    int fd = ::open(path.c_str(), flags, 0666);
    if (-1 == fd)
    {
        ProfileEvents::increment(ProfileEvents::FileOpenFailed);
        if constexpr (!must_exist)
        {
            if (errno == ENOENT)
            {
                return 0;
            }
        }
        throwFromErrno("Cannot open file " + path, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
    }

    return fd;
}

void syncFile(int fd, const std::string & path)
{
    if (-1 == ::fsync(fd))
        throwFromErrno("Cannot fsync " + path, ErrorCodes::CANNOT_FSYNC);
}

void seekFile(int fd, off_t pos, const std::string & path)
{
    ProfileEvents::increment(ProfileEvents::Seek);

    off_t res = lseek(fd, pos, SEEK_SET);
    if (-1 == res)
        throwFromErrno("Cannot seek through file " + path, ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
}

void writeFile(int fd, UInt64 offset, const char * data, size_t to_write, const std::string & path)
{
    ProfileEvents::increment(ProfileEvents::PSMWriteCalls);
    ProfileEvents::increment(ProfileEvents::PSMWriteBytes, to_write);

    size_t bytes_written = 0;
    while (bytes_written != to_write)
    {
        ProfileEvents::increment(ProfileEvents::PSMWriteIOCalls);
        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
            res = ::pwrite(fd, data + bytes_written, to_write - bytes_written, offset + bytes_written);
        }

        if ((-1 == res || 0 == res) && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::PSMWriteFailed);
            throwFromErrno("Cannot write to file " + path, ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_written += res;
    }
}


void readFile(int fd, const off_t offset, const char * buf, size_t expected_bytes, const std::string & path)
{
    ProfileEvents::increment(ProfileEvents::PSMReadCalls);

    size_t bytes_read = 0;
    while (bytes_read < expected_bytes)
    {
        ProfileEvents::increment(ProfileEvents::PSMReadIOCalls);

        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Read};
            res = ::pread(fd, const_cast<char *>(buf + bytes_read), expected_bytes - bytes_read, offset + bytes_read);
        }
        if (!res)
            break;

        if (-1 == res && errno != EINTR)
        {
            ProfileEvents::increment(ProfileEvents::PSMReadFailed);
            throwFromErrno("Cannot read from file " + path, ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
        }

        if (res > 0)
            bytes_read += res;
    }
    ProfileEvents::increment(ProfileEvents::PSMReadBytes, bytes_read);

    if (unlikely(bytes_read != expected_bytes))
        throw Exception("Not enough data in file " + path, ErrorCodes::FILE_SIZE_NOT_MATCH);
}

/// Write and advance sizeof(T) bytes.
template <typename T>
inline void put(char *& pos, const T & v)
{
    std::memcpy(pos, reinterpret_cast<const char *>(&v), sizeof(T));
    pos += sizeof(T);
}

/// Read and advance sizeof(T) bytes.
template <typename T, bool advance = true>
inline T get(std::conditional_t<advance, char *&, const char *> pos)
{
    T v;
    std::memcpy(reinterpret_cast<char *>(&v), pos, sizeof(T));
    if constexpr (advance)
        pos += sizeof(T);
    return v;
}

template <typename C, typename T = typename C::value_type>
std::unique_ptr<C> readValuesFromFile(const std::string & path, Allocator<false> & allocator)
{
    Poco::File file(path);
    if (!file.exists())
        return {};

    size_t file_size = file.getSize();
    int    file_fd   = openFile<true>(path);
    char * data      = (char *)allocator.alloc(file_size);
    SCOPE_EXIT({ allocator.free(data, file_size); });
    char * pos = data;

    readFile(file_fd, 0, data, file_size, path);

    auto               size   = get<UInt64>(pos);
    std::unique_ptr<C> values = std::make_unique<C>();
    for (size_t i = 0; i < size; ++i)
    {
        T v = get<T>(pos);
        values->push_back(v);
    }

    if (unlikely(pos != data + file_size))
        throw Exception("pos not match", ErrorCodes::FILE_SIZE_NOT_MATCH);

    return values;
}

// =========================================================
// Page Meta format
// =========================================================

namespace PageMetaFormat
{
using WBSize          = UInt32;
using PageFileVersion = PageFile::Version;
using PageTag         = UInt64;
using IsPut           = std::underlying_type<WriteBatch::WriteType>::type;
using PageOffset      = UInt64;
using PageSize        = UInt32;
using Checksum        = UInt64;

static const size_t PAGE_META_SIZE = sizeof(PageId) + sizeof(PageTag) + sizeof(PageOffset) + sizeof(PageSize) + sizeof(Checksum);

/// Return <data to write into meta file, data to write into data file>.
std::pair<ByteBuffer, ByteBuffer> genWriteData( //
    const WriteBatch & wb,
    PageFile &         page_file,
    PageEntriesEdit &  edit)
{
    WBSize meta_write_bytes = 0;
    size_t data_write_bytes = 0;

    meta_write_bytes += sizeof(WBSize) + sizeof(PageFileVersion);

    for (const auto & write : wb.getWrites())
    {
        meta_write_bytes += sizeof(IsPut);
        switch (write.type)
        {
        case WriteBatch::WriteType::PUT:
            data_write_bytes += write.size;
            meta_write_bytes += PAGE_META_SIZE;
            break;
        case WriteBatch::WriteType::DEL:
            // For delete page, store page id only. And don't need to write data file.
            meta_write_bytes += sizeof(PageId);
            break;
        case WriteBatch::WriteType::REF:
            // For ref page, store RefPageId -> PageId. And don't need to write data file.
            meta_write_bytes += (sizeof(PageId) + sizeof(PageId));
            break;
        }
    }

    meta_write_bytes += sizeof(Checksum);

    char * meta_buffer = (char *)page_file.alloc(meta_write_bytes);
    char * data_buffer = (char *)page_file.alloc(data_write_bytes);

    char * meta_pos = meta_buffer;
    char * data_pos = data_buffer;

    put(meta_pos, meta_write_bytes);
    put(meta_pos, PageFile::CURRENT_VERSION);

    PageOffset page_data_file_off = page_file.getDataFileAppendPos();
    for (const auto & write : wb.getWrites())
    {
        put(meta_pos, static_cast<IsPut>(write.type));
        switch (write.type)
        {
        case WriteBatch::WriteType::PUT:
        {
            write.read_buffer->readStrict(data_pos, write.size);
            Checksum page_checksum = CityHash_v1_0_2::CityHash64(data_pos, write.size);
            data_pos += write.size;

            PageEntry pc{};
            pc.file_id  = page_file.getFileId();
            pc.level    = page_file.getLevel();
            pc.size     = write.size;
            pc.offset   = page_data_file_off;
            pc.checksum = page_checksum;

            edit.put(write.page_id, pc);

            put(meta_pos, (PageId)write.page_id);
            put(meta_pos, (PageTag)write.tag);
            put(meta_pos, (PageOffset)page_data_file_off);
            put(meta_pos, (PageSize)write.size);
            put(meta_pos, (Checksum)page_checksum);

            page_data_file_off += write.size;
            break;
        }
        case WriteBatch::WriteType::DEL:
            put(meta_pos, (PageId)write.page_id);

            edit.del(write.page_id);
            break;
        case WriteBatch::WriteType::REF:
            put(meta_pos, static_cast<PageId>(write.page_id));
            put(meta_pos, static_cast<PageId>(write.ori_page_id));

            edit.ref(write.page_id, write.ori_page_id);
            break;
        }
    }

    const Checksum wb_checksum = CityHash_v1_0_2::CityHash64(meta_buffer, meta_write_bytes - sizeof(Checksum));
    put(meta_pos, wb_checksum);

    if (unlikely(meta_pos != meta_buffer + meta_write_bytes || data_pos != data_buffer + data_write_bytes))
        throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);

    return {{meta_buffer, meta_pos}, {data_buffer, data_pos}};
}

/// Analyze meta file, and return <available meta size, available data size>.
std::pair<UInt64, UInt64> analyzeMetaFile( //
    const String &    path,
    PageFileId        file_id,
    UInt32            level,
    const char *      meta_data,
    const size_t      meta_data_size,
    PageEntriesEdit & edit,
    Logger *          log)
{
    const char * meta_data_end = meta_data + meta_data_size;

    UInt64 page_data_file_size = 0;
    char * pos                 = const_cast<char *>(meta_data);
    while (pos < meta_data_end)
    {
        if (pos + sizeof(WBSize) > meta_data_end)
        {
            LOG_WARNING(log, "Incomplete write batch, ignored.");
            break;
        }
        const char * wb_start_pos = pos;
        const auto   wb_bytes     = get<WBSize>(pos);
        if (wb_start_pos + wb_bytes > meta_data_end)
        {
            LOG_WARNING(log, "Incomplete write batch, ignored.");
            break;
        }

        // this field is always true now
        const auto version = get<PageFileVersion>(pos);
        if (version != PageFile::CURRENT_VERSION)
            throw Exception("Version not match", ErrorCodes::LOGICAL_ERROR);

        // check the checksum of WriteBatch
        const auto wb_bytes_without_checksum = wb_bytes - sizeof(Checksum);
        const auto wb_checksum               = get<Checksum, false>(wb_start_pos + wb_bytes_without_checksum);
        if (wb_checksum != CityHash_v1_0_2::CityHash64(wb_start_pos, wb_bytes_without_checksum))
        {
            throw Exception("Write batch checksum not match, path: " + path + ", offset: " + DB::toString(wb_start_pos - meta_data),
                            ErrorCodes::CHECKSUM_DOESNT_MATCH);
        }

        // recover WriteBatch
        while (pos < wb_start_pos + wb_bytes_without_checksum)
        {
            const auto is_put     = get<IsPut>(pos);
            const auto write_type = static_cast<WriteBatch::WriteType>(is_put);
            switch (write_type)
            {
            case WriteBatch::WriteType::PUT:
            {
                auto      page_id = get<PageId>(pos);
                PageEntry pc;
                pc.file_id  = file_id;
                pc.level    = level;
                pc.tag      = get<PageTag>(pos);
                pc.offset   = get<PageOffset>(pos);
                pc.size     = get<PageSize>(pos);
                pc.checksum = get<Checksum>(pos);

                edit.put(page_id, pc);
                page_data_file_size += pc.size;
                break;
            }
            case WriteBatch::WriteType::DEL:
            {
                auto page_id = get<PageId>(pos);
                edit.del(page_id); // Reserve the order of removal.
                break;
            }
            case WriteBatch::WriteType::REF:
            {
                const auto ref_id  = get<PageId>(pos);
                const auto page_id = get<PageId>(pos);
                edit.ref(ref_id, page_id);
            }
            }
        }
        // move `pos` over the checksum of WriteBatch
        pos += sizeof(Checksum);

        if (pos != wb_start_pos + wb_bytes)
            throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);
    }
    return {pos - meta_data, page_data_file_size};
}
} // namespace PageMetaFormat

// =========================================================
// PageFile::Writer
// =========================================================

PageFile::Writer::Writer(PageFile & page_file_, bool sync_on_write_)
    : page_file(page_file_),
      sync_on_write(sync_on_write_),
      data_file_path(page_file.dataPath()),
      meta_file_path(page_file.metaPath()),
      data_file_fd(openFile<false>(data_file_path)),
      meta_file_fd(openFile<false>(meta_file_path))
{
}

PageFile::Writer::~Writer()
{
    SCOPE_EXIT({
        ::close(data_file_fd);
        ::close(meta_file_fd);
    });
    syncFile(data_file_fd, data_file_path);
    syncFile(meta_file_fd, meta_file_path);
}

void PageFile::Writer::write(const WriteBatch & wb, PageEntriesEdit & edit)
{
    ProfileEvents::increment(ProfileEvents::PSMWritePages, wb.putWriteCount());

    // TODO: investigate if not copy data into heap, write big pages can be faster?
    ByteBuffer meta_buf, data_buf;
    std::tie(meta_buf, data_buf) = PageMetaFormat::genWriteData(wb, page_file, edit);

    SCOPE_EXIT({ page_file.free(meta_buf.begin(), meta_buf.size()); });
    SCOPE_EXIT({ page_file.free(data_buf.begin(), data_buf.size()); });

    auto write_buf = [&](int fd, UInt64 offset, const std::string & path, ByteBuffer buf) {
        writeFile(fd, offset, buf.begin(), buf.size(), path);
        if (sync_on_write)
            syncFile(fd, path);
    };

    write_buf(data_file_fd, page_file.data_file_pos, data_file_path, data_buf);
    write_buf(meta_file_fd, page_file.meta_file_pos, meta_file_path, meta_buf);

    page_file.data_file_pos += data_buf.size();
    page_file.meta_file_pos += meta_buf.size();
}

// =========================================================
// PageFile::Reader
// =========================================================

PageFile::Reader::Reader(PageFile & page_file) : data_file_path(page_file.dataPath()), data_file_fd(openFile<true>(data_file_path)) {}

PageFile::Reader::~Reader()
{
    ::close(data_file_fd);
}

PageMap PageFile::Reader::read(PageIdAndEntries & to_read)
{
    ProfileEvents::increment(ProfileEvents::PSMReadPages, to_read.size());

    // Sort in ascending order by offset in file.
    std::sort(to_read.begin(), to_read.end(), [](const PageIdAndEntry & a, const PageIdAndEntry & b) {
        return a.second.offset < b.second.offset;
    });

    // allocate data_buf that can hold all pages
    size_t buf_size = 0;
    for (const auto & p : to_read)
    {
        buf_size += p.second.size;
    }
    // TODO optimization:
    // 1. Succeeding pages can be read by one call.
    // 2. Pages with small gaps between them can also read together.
    // 3. Refactor this function to support iterator mode, and then use hint to do data pre-read.

    char *    data_buf   = (char *)alloc(buf_size);
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });

    char *  pos = data_buf;
    PageMap page_map;
    for (const auto & [page_id, page_cache] : to_read)
    {
        readFile(data_file_fd, page_cache.offset, pos, page_cache.size, data_file_path);

        if constexpr (PAGE_CHECKSUM_ON_READ)
        {
            auto checksum = CityHash_v1_0_2::CityHash64(pos, page_cache.size);
            if (checksum != page_cache.checksum)
            {
                throw Exception("Page [" + DB::toString(page_id) + "] checksum not match, broken file: " + data_file_path,
                                ErrorCodes::CHECKSUM_DOESNT_MATCH);
            }
        }

        Page page;
        page.page_id    = page_id;
        page.data       = ByteBuffer(pos, pos + page_cache.size);
        page.mem_holder = mem_holder;
        page_map.emplace(page_id, page);

        pos += page_cache.size;
    }

    if (unlikely(pos != data_buf + buf_size))
        throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);

    return page_map;
}

void PageFile::Reader::read(PageIdAndEntries & to_read, const PageHandler & handler)
{
    ProfileEvents::increment(ProfileEvents::PSMReadPages, to_read.size());

    // Sort in ascending order by offset in file.
    std::sort(to_read.begin(), to_read.end(), [](const PageIdAndEntry & a, const PageIdAndEntry & b) {
        return a.second.offset < b.second.offset;
    });

    size_t buf_size = 0;
    for (const auto & p : to_read)
        buf_size = std::max(buf_size, p.second.size);

    char *    data_buf   = (char *)alloc(buf_size);
    MemHolder mem_holder = createMemHolder(data_buf, [&, buf_size](char * p) { free(p, buf_size); });


    auto it = to_read.begin();
    while (it != to_read.end())
    {
        auto && [page_id, page_cache] = *it;

        readFile(data_file_fd, page_cache.offset, data_buf, page_cache.size, data_file_path);

        if constexpr (PAGE_CHECKSUM_ON_READ)
        {
            auto checksum = CityHash_v1_0_2::CityHash64(data_buf, page_cache.size);
            if (checksum != page_cache.checksum)
            {
                throw Exception("Page [" + DB::toString(page_id) + "] checksum not match, broken file: " + data_file_path,
                                ErrorCodes::CHECKSUM_DOESNT_MATCH);
            }
        }

        Page page;
        page.page_id    = page_id;
        page.data       = ByteBuffer(data_buf, data_buf + page_cache.size);
        page.mem_holder = mem_holder;

        ++it;

#ifndef __APPLE__
        if (it != to_read.end())
        {
            auto & next_page_cache = it->second;
            ::posix_fadvise(data_file_fd, next_page_cache.offset, next_page_cache.size, POSIX_FADV_WILLNEED);
        }
#endif

        handler(page_id, page);
    }
}

// =========================================================
// PageFile
// =========================================================

const PageFile::Version PageFile::CURRENT_VERSION = 1;

PageFile::PageFile(PageFileId file_id_, UInt32 level_, const std::string & parent_path, bool is_tmp_, bool is_create, Logger * log_)
    : file_id(file_id_), level(level_), is_tmp(is_tmp_), parent_path(parent_path), data_file_pos(0), meta_file_pos(0), log(log_)
{
    if (is_create)
    {
        Poco::File file(folderPath());
        if (file.exists())
            file.remove(true);
        file.createDirectories();
    }
}

std::pair<PageFile, bool> PageFile::recover(const std::string & parent_path, const std::string & page_file_name, Logger * log)
{

    if (!startsWith(page_file_name, ".tmp.page_") && !startsWith(page_file_name, "page_"))
    {
        LOG_INFO(log, "Not page file, ignored " + page_file_name);
        return {{}, false};
    }
    std::vector<std::string> ss;
    boost::split(ss, page_file_name, boost::is_any_of("_"));
    if (ss.size() != 3)
    {
        LOG_INFO(log, "Unrecognized file, ignored: " + page_file_name);
        return {{}, false};
    }
    if (ss[0] == ".tmp.page")
    {
        LOG_INFO(log, "Temporary page file, ignored: " + page_file_name);
        return {{}, false};
    }
    // ensure both meta && data exist
    PageFileId file_id = std::stoull(ss[1]);
    UInt32     level   = std::stoi(ss[2]);
    PageFile   pf(file_id, level, parent_path, false, false, log);
    if (!Poco::File(pf.metaPath()).exists())
    {
        LOG_INFO(log, "Broken page without meta file, ignored: " + pf.metaPath());
        return {{}, false};
    }
    if (!Poco::File(pf.dataPath()).exists())
    {
        LOG_INFO(log, "Broken page without data file, ignored: " + pf.dataPath());
        return {{}, false};
    }

    return {pf, true};
}

PageFile PageFile::newPageFile(PageFileId file_id, UInt32 level, const std::string & parent_path, bool is_tmp, Logger * log)
{
    return PageFile(file_id, level, parent_path, is_tmp, true, log);
}

PageFile PageFile::openPageFileForRead(PageFileId file_id, UInt32 level, const std::string & parent_path, Logger * log)
{
    return PageFile(file_id, level, parent_path, false, false, log);
}

void PageFile::readAndSetPageMetas(PageEntriesEdit & edit)
{
    const auto   path = metaPath();
    Poco::File   file(path);
    const size_t file_size = file.getSize();

    int file_fd = openFile<true, false>(path);
    // File not exists.
    if (!file_fd)
        return;
    char * data = (char *)alloc(file_size);
    SCOPE_EXIT({ free(data, file_size); });

    readFile(file_fd, 0, data, file_size, path);

    // analyze meta file and update page_entries
    std::tie(this->meta_file_pos, this->data_file_pos)
        = PageMetaFormat::analyzeMetaFile(folderPath(), file_id, level, data, file_size, edit, log);
}

void PageFile::setFormal()
{
    if (!is_tmp)
        return;
    Poco::File file(folderPath());
    is_tmp = false;
    file.renameTo(folderPath());
}

void PageFile::destroy() const
{
    // TODO: delay remove.
    Poco::File file(folderPath());
    if (file.exists())
    {
        // remove meta first, then remove data
        Poco::File meta_file(metaPath());
        if (meta_file.exists())
        {
            meta_file.remove();
        }
        Poco::File data_file(dataPath());
        if (data_file.exists())
        {
            data_file.remove();
        }
        // drop dir
        file.remove(true);
    }
}

UInt64 PageFile::getDataFileSize() const
{
    Poco::File file(dataPath());
    return file.getSize();
}

} // namespace DB
