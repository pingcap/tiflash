#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileProvider.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/Page/PageUtil.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

namespace DB
{
namespace DM
{

static constexpr const char * NGC_FILE_NAME = "NGC";

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
        LOG_WARNING(log, "Existing dmfile, removed :" << path);
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

    WriteBufferFromFileProvider buf(file_provider, tmp_meta_path, encryptionMetaPath(), false, 4096);
    writeString("DTFile format: ", buf);
    writeIntText(static_cast<std::underlying_type_t<DMFileVersion>>(DMFileVersion::CURRENT_VERSION), buf);
    writeString("\n", buf);
    writeText(column_stats, CURRENT_VERSION, buf);

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
    if (status != Status::WRITING)
        throw Exception("Expected WRITING status, now " + statusString(status));
    Poco::File old_file(path());
    status = Status::READABLE;

    auto new_path = path();

    Poco::File file(new_path);
    if (file.exists())
        file.remove(true);
    old_file.renameTo(new_path);
}

std::set<UInt64> DMFile::listAllInPath(const String & parent_path, bool can_gc)
{
    Poco::File folder(parent_path);
    if (!folder.exists())
        return {};
    std::vector<std::string> file_names;
    folder.list(file_names);
    std::set<UInt64> file_ids;
    Logger *         log = &Logger::get("DMFile");
    for (auto & name : file_names)
    {
        if (!startsWith(name, "dmf_"))
            continue;
        std::vector<std::string> ss;
        boost::split(ss, name, boost::is_any_of("_"));
        if (ss.size() != 2)
        {
            LOG_INFO(log, "Unrecognized DM file, ignored: " + name);
            continue;
        }
        UInt64 file_id = std::stoull(ss[1]);
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
    file_provider->deleteFile(path(), EncryptionPath(encryptionBasePath(), ""));
}


} // namespace DM
} // namespace DB
