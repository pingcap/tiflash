#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPart.h>
#include <Storages/MergeTree/TMTDataPartProperty.h>

namespace DB
{

static ReadBufferFromFile openForReading(const String & path)
{
    return ReadBufferFromFile(path, std::min(static_cast<Poco::File::FileSize>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path).getSize()));
}

static const String TMTDataPartPropertyFileName = "tmt.prop";

void TMTDataPartProperty::load(const MergeTreeData & storage, const String & part_path)
{
    String file_name = part_path + TMTDataPartPropertyFileName;
    ReadBufferFromFile file = openForReading(file_name);
    UInt32 tmt_type;
    const DataTypePtr & type = storage.primary_key_data_types[0];
    for (bool over = false; !over;)
    {
        readBinary(tmt_type, file);
        switch ((TMTDataPartPropertyType)tmt_type)
        {
            case MIN_MAX_PK:
                type->deserializeBinary(min_pk, file);
                type->deserializeBinary(max_pk, file);
                break;
            case END:
                over = true;
                break;
            default:
                throw Exception("[TMTDataPartProperty::load] got invalid type: " + DB::toString(tmt_type), ErrorCodes::LOGICAL_ERROR);
        }
    }
    initialized = true;
}

void TMTDataPartProperty::store(const MergeTreeData & storage, const String & part_path, Checksums & checksums) const
{
    if (!initialized)
        throw Exception(
            "Attempt to store uninitialized TMT property for part " + part_path + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);

    const String & file_name = TMTDataPartPropertyFileName;

    const DataTypePtr & type = storage.primary_key_data_types[0];

    WriteBufferFromFile out(part_path + file_name);
    HashingWriteBuffer out_hashing(out);

    writeBinary<UInt32>(MIN_MAX_PK, out_hashing);
    type->serializeBinary(min_pk, out_hashing);
    type->serializeBinary(max_pk, out_hashing);
    writeBinary<UInt32>(END, out_hashing);
    out_hashing.next();
    checksums.files[file_name].file_size = out_hashing.count();
    checksums.files[file_name].file_hash = out_hashing.getHash();
}

void TMTDataPartProperty::update(const Block & block, const std::string & pk_name)
{
    Field min_value;
    Field max_value;
    const ColumnWithTypeAndName & column = block.getByName(pk_name);
    column.column->getExtremes(min_value, max_value);

    if (!initialized)
    {
        min_pk = Field(min_value);
        max_pk = Field(max_value);
    }
    else
    {
        min_pk = std::min(min_pk, min_value);
        max_pk = std::max(max_pk, max_value);
    }

    initialized = true;
}

void TMTDataPartProperty::merge(const TMTDataPartProperty & other)
{
    if (!other.initialized)
        return;

    if (!initialized)
    {
        min_pk = other.min_pk;
        max_pk = other.max_pk;
        initialized = true;
    }
    else
    {

        min_pk = std::min(min_pk, other.min_pk);
        max_pk = std::max(max_pk, other.max_pk);
    }
}

} // namespace DB
