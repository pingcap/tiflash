#include <Storages/Transaction/HashCheckHelper.h>
#include <Storages/Transaction/RegionFile.h>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT_VERSION;
extern const int CHECKSUM_DOESNT_MATCH;
} // namespace ErrorCodes

static constexpr size_t REGION_FILE_INDEX_BUFFER_SIZE = 24576;

const UInt32 RegionFile::CURRENT_VERSION = 0;

RegionFile::Writer::Writer(RegionFile & region_file)
    : data_file_size(region_file.file_size),
      data_file_buf(region_file.dataPath(), DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_WRONLY | O_CREAT),
      index_file_buf(region_file.indexPath(), REGION_FILE_INDEX_BUFFER_SIZE, O_APPEND | O_WRONLY | O_CREAT)
{}

RegionFile::Writer::~Writer()
{
    // flush page cache.
    data_file_buf.sync();
    index_file_buf.sync();
}

size_t RegionFile::Writer::write(const RegionPtr & region)
{
    HashingWriteBuffer hash_buf(data_file_buf);
    size_t region_size = region->serialize(hash_buf);
    auto hashcode = hash_buf.getHash();

    // index file format: [ version(4 bytes), region_id(8 bytes), region_size(8 bytes), region hash(16 bytes] , [ ... ] ...
    writeIntBinary(RegionFile::CURRENT_VERSION, index_file_buf);
    writeIntBinary(region->id(), index_file_buf);
    writeIntBinary(region_size, index_file_buf);
    writeIntBinary(hashcode.first, index_file_buf);
    writeIntBinary(hashcode.second, index_file_buf);

    data_file_size += region_size;

    return region_size;
}

RegionFile::Reader::Reader(RegionFile & region_file)
    : data_path(region_file.dataPath()),
      data_file_buf(data_path, std::min(region_file.file_size, static_cast<off_t>(DBMS_DEFAULT_BUFFER_SIZE)), O_RDONLY)
{
    ReadBufferFromFile index_file_buf(region_file.indexPath(), REGION_FILE_INDEX_BUFFER_SIZE, O_RDONLY);
    while (!index_file_buf.eof())
    {
        auto version = readBinary2<UInt32>(index_file_buf);
        if (version != RegionFile::CURRENT_VERSION)
            throw Exception(
                "Unexpected region file index version: " + DB::toString(version) + ", expected: " + DB::toString(CURRENT_VERSION));
        auto region_id = readBinary2<UInt64>(index_file_buf);
        auto region_size = readBinary2<UInt64>(index_file_buf);

        HashingWriteBuffer::uint128 hashcode;
        readIntBinary(hashcode.first, index_file_buf);
        readIntBinary(hashcode.second, index_file_buf);

        metas.emplace_back(region_id, region_size, hashcode);
    }
}

void RegionFile::Reader::checkHash(std::vector<bool> use)
{
    std::vector<size_t> region_bytes(use.size());
    std::vector<HashingWriteBuffer::uint128> expected_hashcodes(use.size());
    for (size_t index = 0; index < use.size(); ++index)
    {
        region_bytes[index] = metas[index].region_size;
        expected_hashcodes[index] = metas[index].hashcode;
    }
    FileHashCheck::checkObjectHashInFile(data_path, region_bytes, use, expected_hashcodes);
}

RegionID RegionFile::Reader::hasNext()
{
    if (next_region_index >= metas.size())
        return InvalidRegionID;
    next_region_meta = &metas[next_region_index++];
    return next_region_meta->region_id;
}

RegionPtr RegionFile::Reader::next()
{
    next_region_offset += next_region_meta->region_size;
    return Region::deserialize(data_file_buf);
}

void RegionFile::Reader::skipNext()
{
    next_region_offset += next_region_meta->region_size;
    data_file_buf.seek(next_region_offset);
}

bool RegionFile::tryCoverRegion(RegionID region_id, RegionFile & other)
{
    if (other.file_id == file_id) // myself
        return true;
    if (other.regions.find(region_id) == other.regions.end())
        return true;
    // Now both this and other contains the region, the bigger file_id wins.
    if (other.file_id > file_id)
        regions.erase(region_id); // cover myself
    else
        other.regions.erase(region_id); // cover other
    return false;
}

bool RegionFile::addRegion(RegionID region_id, size_t region_size) { return !regions.insert_or_assign(region_id, region_size).second; }

bool RegionFile::dropRegion(RegionID region_id) { return regions.erase(region_id) != 0; }

/// Remove underlying file and clean up resources.
void RegionFile::destroy()
{
    {
        Poco::File file(indexPath());
        if (file.exists())
            file.remove();
    }

    {
        Poco::File file(dataPath());
        if (file.exists())
            file.remove();
    }

    regions.clear();
    file_size = 0;
}

void RegionFile::resetId(UInt64 new_file_id)
{
    // TODO This could be partial succeed, need to fix.
    {
        Poco::File file(indexPath());
        if (file.exists())
            file.renameTo(indexPath(new_file_id));
    }

    {
        Poco::File file(dataPath());
        if (file.exists())
            file.renameTo(dataPath(new_file_id));
    }

    file_id = new_file_id;
}

Float64 RegionFile::useRate()
{
    size_t size = 0;
    for (auto & p : regions)
        size += p.second;
    return ((Float64)size) / file_size;
}

std::string RegionFile::dataPath() { return dataPath(file_id); }

std::string RegionFile::indexPath() { return indexPath(file_id); }

std::string RegionFile::dataPath(UInt64 the_file_id) { return parent_path + DB::toString(the_file_id) + REGION_DATA_FILE_SUFFIX; }

std::string RegionFile::indexPath(UInt64 the_file_id) { return parent_path + DB::toString(the_file_id) + REGION_INDEX_FILE_SUFFIX; }

} // namespace DB
