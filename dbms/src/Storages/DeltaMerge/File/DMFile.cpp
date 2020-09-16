#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadHelpers.h>
#include <Encryption/WriteBufferFromFileProvider.h>
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
    if (mode == Mode::SINGLE_FILE)
    {
        return path() + "." + NGC_FILE_NAME;
    }
    else
    {
        return path() + "/" + NGC_FILE_NAME;
    }
}

DMFilePtr DMFile::create(const FileProviderPtr & file_provider, UInt64 file_id, const String & parent_path)
{
    Logger * log = &Logger::get("DMFile");
    // On create, ref_id is the same as file_id.
    DMFilePtr new_dmfile(new DMFile(file_id, file_id, parent_path, Status::WRITABLE, log));

    Poco::File parent(parent_path);
    parent.createDirectories();

    auto       path = new_dmfile->path();
    Poco::File file(path);
    if (file.exists())
    {
        file.remove(true);
        LOG_WARNING(log, "Existing dmfile, removed :" << path);
    }
    PageUtil::touchFile(new_dmfile->path());
    new_dmfile->initialize(file_provider);

    // Create a mark file to stop this dmfile from being removed by GC.
    PageUtil::touchFile(new_dmfile->ngcPath());

    return new_dmfile;
}

DMFilePtr DMFile::restore(const FileProviderPtr & file_provider, UInt64 file_id, UInt64 ref_id, const String & parent_path, bool read_meta)
{
    DMFilePtr dmfile(new DMFile(file_id, ref_id, parent_path, Status::READABLE, &Logger::get("DMFile")));
    dmfile->initialize(file_provider);
    if (read_meta)
        dmfile->readMeta(file_provider);
    return dmfile;
}

void DMFile::writeMeta(WriteBufferFromFileBase & buffer)
{
    size_t meta_offset = buffer.count();
    writeString("DTFile format: ", buffer);
    writeIntText(static_cast<std::underlying_type_t<DMFileVersion>>(DMFileVersion::CURRENT_VERSION), buffer);
    writeString("\n", buffer);
    writeText(column_stats, CURRENT_VERSION, buffer);
    size_t meta_size = buffer.count() - meta_offset;
    addSubFileStat(metaIdentifier(), meta_offset, meta_size);
}

void DMFile::writePack(WriteBufferFromFileBase & buffer)
{
    size_t pack_offset = buffer.count();
    for (auto & stat : pack_stats)
    {
        writePODBinary(stat, buffer);
    }
    size_t pack_size = buffer.count() - pack_offset;
    addSubFileStat(packStatIdentifier(), pack_offset, pack_size);
}

void DMFile::writeMeta(const FileProviderPtr & file_provider)
{
    if (unlikely(mode != Mode::FOLDER))
    {
        throw DB::TiFlashException("writeMeta is only expected to be called when mode is FOLDER.", Errors::DeltaTree::Internal);
    }
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
    if (unlikely(mode != Mode::FOLDER))
    {
        throw DB::TiFlashException("UpgradeMetaIfNeed is only expected to be called when mode is FOLDER.", Errors::DeltaTree::Internal);
    }
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
        auto buf = metaReadBuffer(file_provider);
        assertString("DTFile format: ", buf);
        {
            std::underlying_type_t<DMFileVersion> ver_int;
            DB::readText(ver_int, buf);
            ver = static_cast<DMFileVersion>(ver_int);
        }
        assertString("\n", buf);
        readText(column_stats, ver, buf);
        // No need to upgrade meta when mode is Mode::SINGLE_FILE
        if (mode == Mode::FOLDER)
        {
            upgradeMetaIfNeed(file_provider, ver);
        }
    }

    {
        auto pack_stat_size = packStatSize();
        size_t     packs = pack_stat_size / sizeof(PackStat);
        pack_stats.resize(packs);
        auto buf = packStatReadBuffer(file_provider);
        buf.read((char *)pack_stats.data(), sizeof(PackStat) * packs);
    }
}

void DMFile::initialize(const FileProviderPtr & file_provider)
{
    Poco::File file(path());
    if (file.isFile())
    {
        mode = Mode::SINGLE_FILE;
        if (status == Status::READABLE)
        {
            Footer footer;
            ReadBufferFromFileProvider buf(file_provider, path(), EncryptionPath(encryptionBasePath(), ""));
            buf.seek(file.getSize() - sizeof(footer), SEEK_SET);
            // ignore footer.file_format_version
            DB::readIntBinary(footer.sub_file_stat_offset, buf);
            DB::readIntBinary(footer.sub_file_num, buf);

            // initialize sub file state
            buf.seek(footer.sub_file_stat_offset, SEEK_SET);
            SubFileStat sub_file_stat;
            for (UInt32 i = 0; i < footer.sub_file_num; i++)
            {
                DB::readStringBinary(sub_file_stat.name, buf);
                DB::readIntBinary(sub_file_stat.offset, buf);
                DB::readIntBinary(sub_file_stat.size, buf);
                sub_file_stats.emplace(sub_file_stat.name, sub_file_stat);
            }
        }
    }
    else
    {
        mode = Mode::FOLDER;
    }
}

void DMFile::finalize(WriteBufferFromFileBase & buffer)
{
    writeMeta(buffer);
    writePack(buffer);

    Footer footer;
    footer.sub_file_stat_offset = buffer.count();
    footer.sub_file_num = sub_file_stats.size();
    footer.file_format_version = DMSingleFileFormatVersion::SINGLE_FILE_VERSION_BASE;
    for (auto & iter : sub_file_stats)
    {
         writeStringBinary(iter.second.name, buffer);
         writeIntBinary(iter.second.offset, buffer);
         writeIntBinary(iter.second.size, buffer);
    }
    writeIntBinary(footer.sub_file_stat_offset, buffer);
    writeIntBinary(footer.sub_file_num, buffer);
    writeIntBinary(static_cast<std::underlying_type_t<DMSingleFileFormatVersion>>(footer.file_format_version), buffer);
    buffer.next();
    if (status != Status::WRITING)
        throw Exception("Expected WRITING status, now " + statusString(status));
    Poco::File old_file(path());
    Poco::File old_ngc_file(ngcPath());
    status = Status::READABLE;

    auto new_path = path();
    Poco::File file(new_path);
    if (file.exists())
        file.remove();
    Poco::File new_ngc_file(ngcPath());
    new_ngc_file.createFile();
    old_file.renameTo(new_path);
    old_ngc_file.remove();
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
            Poco::File file(parent_path + "/" + name);
            String ngc_path = "";
            if (file.isFile())
            {
                ngc_path = parent_path + "/" + name + "." + NGC_FILE_NAME;
            }
            else
            {
                ngc_path = parent_path + "/" + name + "/" + NGC_FILE_NAME;
            }
            Poco::File ngc_file(ngc_path);
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
    if (mode == Mode::SINGLE_FILE)
    {
        file_provider->deleteRegularFile(path(), EncryptionPath(encryptionBasePath(), ""));
    }
    else
    {
        file_provider->deleteDirectory(path(), true, true);
    }
}

ReadBufferFromFileProvider DMFile::metaReadBuffer(const FileProviderPtr & file_provider)
{
    if (mode == Mode::SINGLE_FILE)
    {
        ReadBufferFromFileProvider buf(file_provider, path(), EncryptionPath(encryptionBasePath(), ""));
        auto meta_offset = sub_file_stats[metaIdentifier()].offset;
        buf.seek(meta_offset);
        return buf;
    }
    else
    {
        return openForRead(file_provider, metaPath(), encryptionMetaPath());
    }
}

ReadBufferFromFileProvider DMFile::packStatReadBuffer(const FileProviderPtr & file_provider)
{
    if (mode == Mode::SINGLE_FILE)
    {
        ReadBufferFromFileProvider buf(file_provider, path(), EncryptionPath(encryptionBasePath(), ""));
        auto pack_stat_offset = sub_file_stats[packStatIdentifier()].offset;
        buf.seek(pack_stat_offset);
        return buf;
    }
    else
    {
        return openForRead(file_provider, packStatPath(), encryptionPackStatPath());
    }
}

size_t DMFile::packStatSize()
{
    if (mode == Mode::SINGLE_FILE)
    {
        return sub_file_stats[packStatIdentifier()].size;
    }
    else
    {
        auto       pack_stat_path = packStatPath();
        Poco::File pack_stat_file(pack_stat_path);
        return pack_stat_file.getSize();
    }
}

} // namespace DM
} // namespace DB
