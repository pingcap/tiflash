#include <Poco/File.h>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Common/StringUtils/StringUtils.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Storages/DeltaMerge/File/DMFile.h>

namespace DB
{
namespace DM
{

DMFilePtr DMFile::create(UInt64 file_id, const String & parent_path)
{
    DMFilePtr new_dmfile(new DMFile(file_id, parent_path, Status::WRITABLE, &Logger::get("DMFile")));

    Poco::File file(new_dmfile->path());
    if (file.exists())
        file.remove(true);
    file.createDirectories();

    return new_dmfile;
}

DMFilePtr DMFile::recover(UInt64 file_id, const String & parent_path)
{
    DMFilePtr dmfile(new DMFile(file_id, parent_path, Status::READABLE, &Logger::get("DMFile")));
    dmfile->readMeta();
    return dmfile;
}

void DMFile::writeMeta()
{
    {
        WriteBufferFromFile buf(metaPath(), 4096);
        writeString("DeltaMergeFile format: 0", buf);
        writeString("\nRows: ", buf);
        DB::writeText(rows, buf);
        writeString("\nChunks: ", buf);
        DB::writeText(getChunks(), buf);
        writeString("\n", buf);
        writeText(column_stats, buf);
    }
    {
        WriteBufferFromFile buf(splitPath());
        buf.write((char *)split.data(), sizeof(ChunkSize) * getChunks());
    }
}

void DMFile::readMeta()
{
    size_t chunks;
    {
        auto buf = openForRead(metaPath());
        assertString("DeltaMergeFile format: 0", buf);
        assertString("\nRows: ", buf);
        DB::readText(rows, buf);
        assertString("\nChunks: ", buf);
        DB::readText(chunks, buf);
        assertString("\n", buf);
        readText(column_stats, buf);
    }
    {
        split.resize(chunks);
        auto buf = openForRead(splitPath());
        buf.read((char *)split.data(), sizeof(ChunkSize) * chunks);
    }
}

void DMFile::finalize()
{
    writeMeta();
    if (status != Status::WRITING)
        throw Exception("Expected WRITING status, now " + statusString(status));
    Poco::File file(path());
    status = Status ::READABLE;
    file.renameTo(path());
}

} // namespace DM
} // namespace DB
