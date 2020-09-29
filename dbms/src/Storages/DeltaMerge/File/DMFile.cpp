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
namespace DM
{

static constexpr const char * NGC_FILE_NAME = "NGC";

String DMFile::ngcPath() const
{
    return path() + (isSingleFileMode() ? "." : "/") + NGC_FILE_NAME;
}

DMFilePtr DMFile::create(UInt64 file_id, const String & parent_path, bool single_file_mode)
{
    Logger * log = &Logger::get("DMFile");
    // On create, ref_id is the same as file_id.
    DMFilePtr new_dmfile(
        new DMFile(file_id, file_id, parent_path, single_file_mode ? Mode::SINGLE_FILE : Mode::FOLDER, Status::WRITABLE, log));

    auto       path = new_dmfile->path();
    Poco::File file(path);
    if (file.exists())
    {
        file.remove(true);
        LOG_WARNING(log, "Existing dmfile, removed :" << path);
    }
    if (single_file_mode)
    {
        Poco::File parent(parent_path);
        parent.createDirectories();
        PageUtil::touchFile(path);
    }
    else
    {
        file.createDirectories();
    }

    // Create a mark file to stop this dmfile from being removed by GC.
    PageUtil::touchFile(new_dmfile->ngcPath());

    return new_dmfile;
}

DMFilePtr DMFile::restore(const FileProviderPtr & file_provider, UInt64 file_id, UInt64 ref_id, const String & parent_path, bool read_meta)
{
    String    path             = parent_path + "/dmf_" + DB::toString(file_id);
    bool      single_file_mode = Poco::File(path).isFile();
    DMFilePtr dmfile(new DMFile(
        file_id, ref_id, parent_path, single_file_mode ? Mode::SINGLE_FILE : Mode::FOLDER, Status::READABLE, &Logger::get("DMFile")));
    if (read_meta)
        dmfile->readMeta(file_provider);
    return dmfile;
}

String DMFile::colIndexCacheKey(const FileNameBase & file_name_base) const
{
    if (isSingleFileMode())
    {
        return path() + "/" + DMFile::colIndexFileName(file_name_base);
    }
    else
    {
        return colIndexPath(file_name_base);
    }
}

String DMFile::colMarkCacheKey(const FileNameBase & file_name_base) const
{
    if (isSingleFileMode())
    {
        return path() + "/" + DMFile::colMarkFileName(file_name_base);
    }
    else
    {
        return colMarkPath(file_name_base);
    }
}

bool DMFile::isColIndexExist(const ColId & col_id) const
{
    if (isSingleFileMode())
    {
        const auto & index_identifier = DMFile::colIndexFileName(DMFile::getFileNameBase(col_id));
        return isSubFileExists(index_identifier);
    }
    else
    {
        auto       index_path = colIndexPath(DMFile::getFileNameBase(col_id));
        Poco::File index_file(index_path);
        return index_file.exists();
    }
}

const EncryptionPath DMFile::encryptionDataPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : file_name_base + ".dat");
}

const EncryptionPath DMFile::encryptionIndexPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : file_name_base + ".idx");
}

const EncryptionPath DMFile::encryptionMarkPath(const FileNameBase & file_name_base) const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : file_name_base + ".mrk");
}

const EncryptionPath DMFile::encryptionMetaPath() const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : "meta.txt");
}

const EncryptionPath DMFile::encryptionPackStatPath() const
{
    return EncryptionPath(encryptionBasePath(), isSingleFileMode() ? "" : "pack");
}

std::tuple<size_t, size_t> DMFile::writeMeta(WriteBuffer & buffer)
{
    size_t meta_offset = buffer.count();
    writeString("DTFile format: ", buffer);
    writeIntText(static_cast<std::underlying_type_t<DMFileVersion>>(DMFileVersion::CURRENT_VERSION), buffer);
    writeString("\n", buffer);
    writeText(column_stats, CURRENT_VERSION, buffer);
    size_t meta_size = buffer.count() - meta_offset;
    return std::make_tuple(meta_offset, meta_size);
}

std::tuple<size_t, size_t> DMFile::writePack(WriteBuffer & buffer)
{
    size_t pack_offset = buffer.count();
    for (auto & stat : pack_stats)
    {
        writePODBinary(stat, buffer);
    }
    size_t pack_size = buffer.count() - pack_offset;
    return std::make_tuple(pack_offset, pack_size);
}

void DMFile::writeMeta(const FileProviderPtr & file_provider)
{
    String meta_path     = metaPath();
    String tmp_meta_path = meta_path + ".tmp";

    WriteBufferFromFileProvider buf(file_provider, tmp_meta_path, encryptionMetaPath(), false, 4096);
    writeMeta(buf);

    Poco::File(tmp_meta_path).renameTo(meta_path);
}

void DMFile::upgradeMetaIfNeed(const FileProviderPtr & file_provider, DMFileVersion ver)
{
    if (unlikely(mode != Mode::FOLDER))
    {
        throw DB::TiFlashException("upgradeMetaIfNeed is only expected to be called when mode is FOLDER.", Errors::DeltaTree::Internal);
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
    size_t meta_offset      = 0;
    size_t meta_size        = 0;
    size_t pack_stat_offset = 0;
    size_t pack_stat_size   = 0;
    if (isSingleFileMode())
    {
        Poco::File                 file(path());
        ReadBufferFromFileProvider buf(file_provider, path(), EncryptionPath(encryptionBasePath(), ""));
        buf.seek(file.getSize() - sizeof(Footer), SEEK_SET);
        DB::readIntBinary(meta_offset, buf);
        DB::readIntBinary(meta_size, buf);
        DB::readIntBinary(pack_stat_offset, buf);
        DB::readIntBinary(pack_stat_size, buf);
    }
    else
    {
        meta_size      = Poco::File(metaPath()).getSize();
        pack_stat_size = Poco::File(packStatPath()).getSize();
    }

    {
        auto buf = openForRead(file_provider, metaPath(), encryptionMetaPath(), meta_size);
        buf.seek(meta_offset);

        DMFileVersion ver; // Binary version
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
        size_t packs = pack_stat_size / sizeof(PackStat);
        pack_stats.resize(packs);
        auto buf = openForRead(file_provider, packStatPath(), encryptionPackStatPath(), pack_stat_size);
        buf.seek(pack_stat_offset);
        buf.readStrict((char *)pack_stats.data(), sizeof(PackStat) * packs);
    }
}

void DMFile::initializeSubFileStatIfNeeded(const FileProviderPtr & file_provider)
{
    std::unique_lock lock(mutex);
    if (!isSingleFileMode() || !sub_file_stats.empty())
        return;

    Poco::File file(path());
    if (status == Status::READABLE)
    {
        Footer                     footer;
        ReadBufferFromFileProvider buf(file_provider, path(), EncryptionPath(encryptionBasePath(), ""));
        buf.seek(file.getSize() - sizeof(Footer) + sizeof(MetaPackInfo), SEEK_SET);
        // ignore footer.file_format_version
        DB::readIntBinary(footer.sub_file_stat_offset, buf);
        DB::readIntBinary(footer.sub_file_num, buf);

        // initialize sub file state
        buf.seek(footer.sub_file_stat_offset, SEEK_SET);
        SubFileStat sub_file_stat;
        for (UInt32 i = 0; i < footer.sub_file_num; i++)
        {
            String name;
            DB::readStringBinary(name, buf);
            DB::readIntBinary(sub_file_stat.offset, buf);
            DB::readIntBinary(sub_file_stat.size, buf);
            sub_file_stats.emplace(name, sub_file_stat);
        }
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

void DMFile::finalize(WriteBuffer & buffer)
{
    Footer footer;
    std::tie(footer.meta_pack_info.meta_offset, footer.meta_pack_info.meta_size)           = writeMeta(buffer);
    std::tie(footer.meta_pack_info.pack_stat_offset, footer.meta_pack_info.pack_stat_size) = writePack(buffer);
    footer.sub_file_stat_offset                                                            = buffer.count();
    footer.sub_file_num                                                                    = sub_file_stats.size();
    footer.file_format_version = DMSingleFileFormatVersion::SINGLE_FILE_VERSION_BASE;
    for (auto & iter : sub_file_stats)
    {
        writeStringBinary(iter.first, buffer);
        writeIntBinary(iter.second.offset, buffer);
        writeIntBinary(iter.second.size, buffer);
    }
    writeIntBinary(footer.meta_pack_info.meta_offset, buffer);
    writeIntBinary(footer.meta_pack_info.meta_size, buffer);
    writeIntBinary(footer.meta_pack_info.pack_stat_offset, buffer);
    writeIntBinary(footer.meta_pack_info.pack_stat_size, buffer);
    writeIntBinary(footer.sub_file_stat_offset, buffer);
    writeIntBinary(footer.sub_file_num, buffer);
    writeIntBinary(static_cast<std::underlying_type_t<DMSingleFileFormatVersion>>(footer.file_format_version), buffer);
    buffer.next();
    if (status != Status::WRITING)
        throw Exception("Expected WRITING status, now " + statusString(status));
    Poco::File old_file(path());
    Poco::File old_ngc_file(ngcPath());
    status = Status::READABLE;

    auto       new_path = path();
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
            String     ngc_path = parent_path + "/" + name + (file.isFile() ? "." : "/") + NGC_FILE_NAME;
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
    if (isSingleFileMode())
    {
        file_provider->deleteRegularFile(path(), EncryptionPath(encryptionBasePath(), ""));
    }
    else
    {
        file_provider->deleteDirectory(path(), true, true);
    }
}

} // namespace DM
} // namespace DB
