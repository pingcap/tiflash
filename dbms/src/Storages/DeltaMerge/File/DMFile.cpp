#include <Common/FailPoint.h>
#include <Common/StringUtils/StringUtils.h>
#include <Encryption/WriteBufferFromFileProvider.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/Page/PageUtil.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace DB
{

namespace FailPoints
{
extern const char exception_before_dmfile_remove_encryption[];
extern const char exception_before_dmfile_remove_from_disk[];
}

namespace DM
{

static constexpr const char * NGC_FILE_NAME = "NGC";

namespace
{
constexpr static const char * FOLDER_PREFIX_WRITABLE = ".tmp.dmf_";
constexpr static const char * FOLDER_PREFIX_READABLE = "dmf_";
constexpr static const char * FOLDER_PREFIX_DROPPED  = ".del.dmf_";

String getPathByStatus(const String & parent_path, UInt64 file_id, DMFile::Status status)
{
    String s = parent_path + "/";
    switch (status)
    {
    case DMFile::Status::READABLE:
        s += FOLDER_PREFIX_READABLE;
        break;
    case DMFile::Status::WRITABLE:
    case DMFile::Status::WRITING:
        s += FOLDER_PREFIX_WRITABLE;
        break;
    case DMFile::Status::DROPPED:
        s += FOLDER_PREFIX_DROPPED;
        break;
    }
    s += DB::toString(file_id);
    return s;
}
} // namespace

String DMFile::path() const
{
    return getPathByStatus(parent_path, file_id, status);
}

String DMFile::ngcPath() const
{
    return path() + "/" + NGC_FILE_NAME;
}

DMFilePtr DMFile::create(UInt64 file_id, const String & parent_path)
{
    Logger * log = &Logger::get("DMFile");
    // On create, ref_id is the same as file_id.
    DMFilePtr new_dmfile(new DMFile(file_id, file_id, parent_path, Status::WRITABLE, log));

    auto       path = new_dmfile->path();
    Poco::File file(path);
    if (file.exists())
    {
        file.remove(true);
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": Existing dmfile, removed: " << path);
    }
    file.createDirectories();

    // Create a mark file to stop this dmfile from being removed by GC.
    PageUtil::touchFile(new_dmfile->ngcPath());

    return new_dmfile;
}

DMFilePtr DMFile::restore(const FileProviderPtr & file_provider, UInt64 file_id, UInt64 ref_id, const String & parent_path, bool read_meta)
{
    DMFilePtr dmfile(new DMFile(file_id, ref_id, parent_path, Status::READABLE, &Logger::get("DMFile")));
    if (read_meta)
        dmfile->readMeta(file_provider);
    return dmfile;
}

void DMFile::writeMeta(const FileProviderPtr & file_provider)
{
    String meta_path     = metaPath();
    String tmp_meta_path = meta_path + ".tmp";

    {
        WriteBufferFromFileProvider buf(file_provider, tmp_meta_path, encryptionMetaPath(), false, 4096);
        writeString("DTFile format: ", buf);
        writeIntText(static_cast<std::underlying_type_t<DMFileVersion>>(DMFileVersion::CURRENT_VERSION), buf);
        writeString("\n", buf);
        writeText(column_stats, CURRENT_VERSION, buf);
        buf.sync();
    }

    Poco::File(tmp_meta_path).renameTo(meta_path);
}

void DMFile::upgradeMetaIfNeed(const FileProviderPtr & file_provider, DMFileVersion ver)
{
    if (unlikely(ver == DMFileVersion::VERSION_BASE))
    {
        // Update ColumnStat.serialized_bytes
        for (auto && c : column_stats)
        {
            auto   col_id = c.first;
            auto & stat   = c.second;
            c.second.type->enumerateStreams(
                [col_id, &stat, this](const IDataType::SubstreamPath & substream) {
                    String stream_name = DMFile::getFileNameBase(col_id, substream);
                    String data_file   = colDataPath(stream_name);
                    if (Poco::File f(data_file); f.exists())
                        stat.serialized_bytes += f.getSize();
                    String mark_file = colDataPath(stream_name);
                    if (Poco::File f(mark_file); f.exists())
                        stat.serialized_bytes += f.getSize();
                    String index_file = colIndexPath(stream_name);
                    if (Poco::File f(index_file); f.exists())
                        stat.serialized_bytes += f.getSize();
                },
                {});
        }
        // Update ColumnStat in meta.
        writeMeta(file_provider);
    }
}

void DMFile::readMeta(const FileProviderPtr & file_provider)
{
    DMFileVersion ver; // Binary version
    {
        auto buf = openForRead(file_provider, metaPath(), encryptionMetaPath());
        assertString("DTFile format: ", buf);
        {
            std::underlying_type_t<DMFileVersion> ver_int;
            DB::readText(ver_int, buf);
            ver = static_cast<DMFileVersion>(ver_int);
        }
        assertString("\n", buf);
        readText(column_stats, ver, buf);

        upgradeMetaIfNeed(file_provider, ver);
    }

    {
        auto       pack_stat_path = packStatPath();
        Poco::File pack_stat_file(pack_stat_path);
        size_t     packs = pack_stat_file.getSize() / sizeof(PackStat);
        pack_stats.resize(packs);
        auto buf = openForRead(file_provider, pack_stat_path, encryptionPackStatPath());
        buf.read((char *)pack_stats.data(), sizeof(PackStat) * packs);
    }
}

void DMFile::finalize(const FileProviderPtr & file_provider)
{
    writeMeta(file_provider);
    if (unlikely(status != Status::WRITING))
        throw Exception("Expected WRITING status, now " + statusString(status));
    Poco::File old_file(path());
    setStatus(Status::READABLE);

    auto new_path = path();

    Poco::File file(new_path);
    if (file.exists())
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": Existing dmfile, removing: " << new_path);
        const String deleted_path = getPathByStatus(parent_path, file_id, Status::DROPPED);
        // no need to delete the encryption info associated with the dmfile path here.
        // because this dmfile path is still a valid path and no obsolete encryption info will be left.
        file.renameTo(deleted_path);
        file.remove(true);
        LOG_WARNING(log, __PRETTY_FUNCTION__ << ": Existing dmfile, removed: " << deleted_path);
    }
    old_file.renameTo(new_path);
}

std::set<UInt64> DMFile::listAllInPath(const FileProviderPtr & file_provider, const String & parent_path, bool can_gc)
{
    Poco::File folder(parent_path);
    if (!folder.exists())
        return {};
    std::vector<std::string> file_names;
    folder.list(file_names);
    std::set<UInt64> file_ids;
    Logger *         log = &Logger::get("DMFile");

    auto try_parse_file_id = [](const String & name) -> std::optional<UInt64> {
        std::vector<std::string> ss;
        boost::split(ss, name, boost::is_any_of("_"));
        if (ss.size() != 2)
            return std::nullopt;
        return std::make_optional(std::stoull(ss[1]));
    };

    for (const auto & name : file_names)
    {

        // clear deleted (maybe broken) DMFiles
        if (startsWith(name, FOLDER_PREFIX_DROPPED))
        {
            auto res = try_parse_file_id(name);
            if (!res)
            {
                LOG_INFO(log, "Unrecognized dropped DM file, ignored: " + name);
                continue;
            }
            UInt64 file_id = *res;
            // The encryption info use readable path. We are not sure the encryption info is deleted or not.
            // Try to delete and ignore if it is already deleted.
            const String readable_path = getPathByStatus(parent_path, file_id, DMFile::Status::READABLE);
            file_provider->deleteEncryptionInfo(EncryptionPath(readable_path, ""), /* throw_on_error= */ false);
            if (Poco::File del_file(parent_path + "/" + name); del_file.exists())
                del_file.remove(true);
            continue;
        }

        if (!startsWith(name, FOLDER_PREFIX_READABLE))
            continue;
        auto res = try_parse_file_id(name);
        if (!res)
        {
            LOG_INFO(log, "Unrecognized DM file, ignored: " + name);
            continue;
        }
        UInt64 file_id = *res;

        if (can_gc)
        {
            Poco::File ngc_file(parent_path + "/" + name + "/" + NGC_FILE_NAME);
            if (!ngc_file.exists())
                file_ids.insert(file_id);
        }
        else
        {
            file_ids.insert(file_id);
        }
    }
    return file_ids;
}

bool DMFile::canGC()
{
    return !Poco::File(ngcPath()).exists();
}

void DMFile::enableGC()
{
    Poco::File ngc_file(ngcPath());
    if (ngc_file.exists())
        ngc_file.remove();
}

void DMFile::remove(const FileProviderPtr & file_provider)
{
    // If we use `FileProvider::deleteDirectory`, it may left a broken DMFile on disk.
    // By renaming DMFile with a prefix first, even if there are broken DMFiles left,
    // we can safely clean them when `DMFile::listAllInPath` is called.
    const String dir_path = path();
    if (Poco::File dir_file(dir_path); dir_file.exists())
    {
        setStatus(Status::DROPPED);
        const String deleted_path = path();
        // Rename the directory first (note that we should do it before deleting encryption info)
        dir_file.renameTo(deleted_path);
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_dmfile_remove_encryption);
        file_provider->deleteEncryptionInfo(EncryptionPath(dir_path, ""));
        FAIL_POINT_TRIGGER_EXCEPTION(FailPoints::exception_before_dmfile_remove_from_disk);
        // Then clean the files on disk
        dir_file.remove(true);
    }
}


} // namespace DM
} // namespace DB
