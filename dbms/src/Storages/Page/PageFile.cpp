#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/randomSeed.h>
#include <common/logger_useful.h>

#include <IO/WriteHelpers.h>

#include <Poco/File.h>
#include <ext/scope_guard.h>

#include <Storages/Page/PageFile.h>

namespace ProfileEvents
{
extern const Event FileOpen;
extern const Event FileOpenFailed;
extern const Event Seek;
extern const Event PSMWritePages;
extern const Event PSMWritePageCalls;
extern const Event PSMWriteIOCalls;
extern const Event PSMWriteBytes;
extern const Event PSMReadPages;
extern const Event PSMReadPageCalls;
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
    int fd;

    int flags;
    if constexpr (read)
    {
        flags = O_RDONLY;
    }
    else
    {
        flags = O_WRONLY | O_CREAT;
    }

    fd = ::open(path.c_str(), flags, 0666);
    if (-1 == fd)
    {
        ProfileEvents::increment(ProfileEvents::FileOpenFailed);
        if constexpr (!must_exist)
        {
            if (errno == ENOENT)
                return 0;
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

void writeFile(int fd, const char * data, size_t to_write, const std::string & path)
{
    ProfileEvents::increment(ProfileEvents::PSMWritePageCalls);
    ProfileEvents::increment(ProfileEvents::PSMWriteBytes, to_write);

    size_t bytes_written = 0;
    while (bytes_written != to_write)
    {
        ProfileEvents::increment(ProfileEvents::PSMWriteIOCalls);
        ssize_t res = 0;
        {
            CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
            res = ::write(fd, data + bytes_written, to_write - bytes_written);
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
    ProfileEvents::increment(ProfileEvents::PSMReadPageCalls);

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

template <typename C, typename T = typename C::value_type>
void writeValuesIntoFile(C values, const std::string & path, Allocator<false> & allocator)
{
    std::string tmp_file_path = path + ".tmp." + DB::toString(randomSeed());

    int fd = openFile<false>(tmp_file_path);

    size_t data_size = sizeof(UInt64) + sizeof(T) * values.size();
    char * data      = (char *)allocator.alloc(data_size);
    SCOPE_EXIT({ allocator.free(data, data_size); });

    put(data, (UInt64)values.size());
    for (const auto & v : values)
    {
        put(data, v);
    }

    writeFile(fd, data, data_size, tmp_file_path);
    syncFile(fd, tmp_file_path);

    Poco::File(tmp_file_path).renameTo(path);
}

// =========================================================
// Page Meta format
// =========================================================

namespace PageMetaFormat
{
using WBSize          = UInt32;
using PageFileVersion = PageFile::Version;
using PageTag         = UInt64;
using IsPut           = UInt8;
using PageOffset      = UInt64;
using PageSize        = UInt32;
using Checksum        = UInt64;

static const size_t PAGE_META_SIZE = sizeof(PageId) + sizeof(PageTag) + sizeof(PageOffset) + sizeof(PageSize) + sizeof(Checksum);

/// Return <data to write into meta file, data to write into data file>.
std::pair<ByteBuffer, ByteBuffer> genWriteData( //
    const WriteBatch & wb,
    PageFile &         page_file,
    PageCacheMap &     page_cache_map)
{
    WBSize meta_write_bytes = 0;
    size_t data_write_bytes = 0;

    meta_write_bytes += sizeof(WBSize) + sizeof(PageFileVersion);

    for (const auto & write : wb.getWrites())
    {
        meta_write_bytes += sizeof(IsPut);
        if (write.is_put)
        {
            data_write_bytes += write.size;
            meta_write_bytes += PAGE_META_SIZE;
        }
        else
        {
            meta_write_bytes += sizeof(PageId); // For delete page, store page id only. And don't need to write data file.
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
        put(meta_pos, (IsPut)(write.is_put ? 1 : 0));
        if (write.is_put)
        {
            write.read_buffer->readStrict(data_pos, write.size);
            Checksum page_checksum = CityHash_v1_0_2::CityHash64(data_pos, write.size);
            data_pos += write.size;

            PageCache pc{};
            pc.file_id  = page_file.getFileId();
            pc.level    = page_file.getLevel();
            pc.size     = write.size;
            pc.offset   = page_data_file_off;
            pc.checksum = page_checksum;

            page_cache_map[write.page_id] = pc;

            put(meta_pos, (PageId)write.page_id);
            put(meta_pos, (PageTag)write.tag);
            put(meta_pos, (PageOffset)page_data_file_off);
            put(meta_pos, (PageSize)write.size);
            put(meta_pos, (Checksum)page_checksum);

            page_data_file_off += write.size;
        }
        else
        {
            put(meta_pos, (PageId)write.page_id);

            page_cache_map.erase(write.page_id);
        }
    }

    Checksum wb_checksum = CityHash_v1_0_2::CityHash64(meta_buffer, meta_write_bytes - sizeof(Checksum));
    put(meta_pos, wb_checksum);

    if (unlikely(meta_pos != meta_buffer + meta_write_bytes || data_pos != data_buffer + data_write_bytes))
        throw Exception("pos not match", ErrorCodes::LOGICAL_ERROR);

    return {{meta_buffer, meta_pos}, {data_buffer, data_pos}};
}

/// Analyze meta file, and return <available meta size, available data size>.
std::pair<UInt64, UInt64> analyzeMetaFile( //
    PageFileId     file_id,
    UInt32         level,
    const char *   meta_data,
    const size_t   meta_data_size,
    PageCacheMap & page_caches,
    Logger *       log)
{
    const char * meta_data_end = meta_data + meta_data_size;

    UInt64 page_data_file_size = 0;
    char * pos                 = const_cast<char *>(meta_data);
    while (pos < meta_data + meta_data_size)
    {
        if (pos + sizeof(WBSize) > meta_data_end)
        {
            LOG_WARNING(log, "Incomplete write batch, ignored.");
            break;
        }
        const char * wb_start_pos = pos;
        auto         wb_bytes     = get<WBSize>(pos);
        if (wb_start_pos + wb_bytes > meta_data_end)
        {
            LOG_WARNING(log, "Incomplete write batch, ignored.");
            break;
        }
        auto wb_bytes_without_checksum = wb_bytes - sizeof(Checksum);

        auto version     = get<PageFileVersion>(pos);
        auto wb_checksum = get<Checksum, false>(wb_start_pos + wb_bytes_without_checksum);

        if (wb_checksum != CityHash_v1_0_2::CityHash64(wb_start_pos, wb_bytes_without_checksum))
            throw Exception("Write batch checksum not match", ErrorCodes::CHECKSUM_DOESNT_MATCH);
        if (version != PageFile::CURRENT_VERSION)
            throw Exception("Version not match", ErrorCodes::LOGICAL_ERROR);

        while (pos < wb_start_pos + wb_bytes_without_checksum)
        {
            auto is_put = get<UInt8>(pos);
            if (is_put)
            {
                PageCache pc{};

                auto page_id = get<PageId>(pos);
                pc.tag       = get<PageTag>(pos);
                pc.offset    = get<PageOffset>(pos);
                pc.size      = get<PageSize>(pos);
                pc.checksum  = get<Checksum>(pos);
                pc.file_id   = file_id;
                pc.level     = level;

                page_caches[page_id] = pc;
                page_data_file_size += pc.size;
            }
            else
            {
                auto page_id = get<PageId>(pos);
                page_caches.erase(page_id); // Reserve the order of removal.
            }
        }
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
    seekFile(data_file_fd, page_file.data_file_pos, page_file.dataPath());
    seekFile(meta_file_fd, page_file.meta_file_pos, page_file.metaPath());
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

void PageFile::Writer::write(const WriteBatch & wb, PageCacheMap & page_cache_map)
{
    ByteBuffer meta_buf, data_buf;
    std::tie(meta_buf, data_buf) = PageMetaFormat::genWriteData(wb, page_file, page_cache_map);

    SCOPE_EXIT({ page_file.free(meta_buf.begin(), meta_buf.size()); });
    SCOPE_EXIT({ page_file.free(data_buf.begin(), data_buf.size()); });

    auto write_buf = [&](int fd, const std::string & path, ByteBuffer buf) {
        writeFile(fd, buf.begin(), buf.size(), path);
        if (sync_on_write)
            syncFile(fd, path);
    };

    write_buf(data_file_fd, data_file_path, data_buf);
    write_buf(meta_file_fd, meta_file_path, meta_buf);

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

PageMap PageFile::Reader::read(PageIdAndCaches & to_read)
{
    // Sort in ascending order by offset in file.
    std::sort(to_read.begin(), to_read.end(), [](const PageIdAndCache & a, const PageIdAndCache & b) {
        return a.second.offset < b.second.offset;
    });

    size_t buf_size = 0;
    for (const auto & p : to_read)
        buf_size += p.second.size;

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
                throw Exception("Page checksum not match, broken file.", ErrorCodes::CHECKSUM_DOESNT_MATCH);
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

    PageFileId file_id = std::stoull(ss[1]);
    UInt32     level   = std::stoi(ss[2]);
    return {PageFile(file_id, level, parent_path, false, false, log), true};
}

PageFile PageFile::newPageFile(PageFileId file_id, UInt32 level, const std::string & parent_path, bool is_tmp, Logger * log)
{
    return PageFile(file_id, level, parent_path, is_tmp, true, log);
}

PageFile PageFile::openPageFileForRead(PageFileId file_id, UInt32 level, const std::string & parent_path, Logger * log)
{
    return PageFile(file_id, level, parent_path, false, false, log);
}

void PageFile::readAndSetPageMetas(PageCacheMap & page_caches)
{
    auto       path = metaPath();
    Poco::File file(path);
    size_t     file_size = file.getSize();

    int file_fd = openFile<true, false>(path);
    // File not exists.
    if (!file_fd)
        return;
    char * data = (char *)alloc(file_size);
    SCOPE_EXIT({ free(data, file_size); });

    readFile(file_fd, 0, data, file_size, path);

    std::tie(this->meta_file_pos, this->data_file_pos) = PageMetaFormat::analyzeMetaFile(file_id, level, data, file_size, page_caches, log);
}

void PageFile::setFormal()
{
    if (!is_tmp)
        return;
    Poco::File file(folderPath());
    is_tmp = false;
    file.renameTo(folderPath());
}

void PageFile::destroy()
{
    // TODO: delay remove.
    Poco::File file(folderPath());
    if (file.exists())
        file.remove(true);
}

UInt64 PageFile::getDataFileSize() const
{
    Poco::File file(dataPath());
    return file.getSize();
}

} // namespace DB
