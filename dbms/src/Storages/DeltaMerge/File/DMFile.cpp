#include <Poco/File.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/Page/PageUtil.h>

namespace DB
{
namespace DM
{

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

DMFilePtr DMFile::restore(UInt64 file_id, UInt64 ref_id, const String & parent_path, bool read_meta)
{
    DMFilePtr dmfile(new DMFile(file_id, ref_id, parent_path, Status::READABLE, &Logger::get("DMFile")));
    if (read_meta)
        dmfile->readMeta();
    return dmfile;
}

void DMFile::writeMeta()
{
    WriteBufferFromFile buf(metaPath(), 4096);
    writeString("DeltaMergeFile format: 0", buf);
    writeString("\n", buf);
    writeText(column_stats, buf);
}

void DMFile::readMeta()
{
    {
        auto buf = openForRead(metaPath());
        assertString("DeltaMergeFile format: 0", buf);
        assertString("\n", buf);
        readText(column_stats, buf);
    }

    {
        auto       chunk_stat_path = chunkStatPath();
        Poco::File chunk_stat_file(chunk_stat_path);
        size_t     chunks = chunk_stat_file.getSize() / sizeof(ChunkStat);
        chunk_stats.resize(chunks);
        auto buf = openForRead(chunk_stat_path);
        buf.read((char *)chunk_stats.data(), sizeof(ChunkStat) * chunks);
    }
}

void DMFile::finalize()
{
    writeMeta();
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

void DMFile::remove()
{
    Poco::File file(path());
    if (file.exists())
        file.remove(true);
}


} // namespace DM
} // namespace DB
