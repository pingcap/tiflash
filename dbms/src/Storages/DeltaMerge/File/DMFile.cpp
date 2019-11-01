#include <Poco/File.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/File/DMFile.h>

namespace DB
{
namespace DM
{

static ReadBufferFromFile openForRead(const String & path)
{
    return ReadBufferFromFile(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
}

DMFilePtr DMFile::create(UInt64 file_id, const String & parent_path)
{
    auto new_dmfile = std::make_shared<DMFile>(file_id, parent_path, Status::WRITABLE, &Logger::get("DMFile"));

    Poco::File file(new_dmfile->path());
    if (file.exists())
        file.remove(true);
    file.createDirectories();

    return new_dmfile;
}

DMFilePtr DMFile::recover(const String & parent_path, const String & file_name)
{
    Logger * log = &Logger::get("DMFile");
    if (!startsWith(file_name, ".tmp.page_") && !startsWith(file_name, "page_"))
    {
        LOG_TRACE(log, "Not dmfile, ignored " + file_name);
        return {};
    }
    std::vector<std::string> ss;
    boost::split(ss, file_name, boost::is_any_of("_"));
    if (ss.size() != 2 || ss[0] != "dmf")
    {
        LOG_TRACE(log, "Unrecognized file, ignored: " + file_name);
        return {};
    }
    if (ss[0] == ".tmp.dmf")
    {
        LOG_TRACE(log, "Temporary dmfile, ignored: " + file_name);
        return {};
    }
    UInt64 file_id = std::stoull(ss[1]);
    auto   dmfile  = std::make_shared<DMFile>(file_id, parent_path, Status::READABLE, log);
    dmfile->readMeta();
    return dmfile;
}

void DMFile::writeMeta()
{
    {
        WriteBufferFromFile buf(metaPath(), 4096);
        writeString("DeltaMergeFile format: 0", buf);
        writeString("\nRows", buf);
        DB::writeText(rows, buf);
        writeString("\nChunks", buf);
        DB::writeText(chunk_sizes.size(), buf);
        writeString("\n", buf);
        writeText(colid_and_types, buf);
    }
    {
        WriteBufferFromFile buf(splitPath(), 4096);
        buf.write((char *)chunk_sizes.data(), chunk_sizes.size() * sizeof(ChunkSize));
    }
}

void DMFile::readMeta()
{
    size_t chunks;
    {
        auto buf = openForRead(metaPath());
        assertString("DeltaMergeFile format: 0", buf);
        assertString("\nRows", buf);
        DB::readText(rows, buf);
        assertString("\nChunks", buf);
        DB::readText(chunks, buf);
        assertString("\n", buf);
        readText(colid_and_types, buf);
    }
    {
        chunk_sizes.resize(chunks);
        auto buf = openForRead(splitPath());
        buf.read((char *)chunk_sizes.data(), chunk_sizes.size() * sizeof(ChunkSize));
    }
}

} // namespace DM
} // namespace DB
