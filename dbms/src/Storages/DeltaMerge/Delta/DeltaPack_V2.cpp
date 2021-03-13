#include <Storages/DeltaMerge/Delta/DeltaPack.h>
#include <Storages/DeltaMerge/Delta/DeltaPackBlock.h>
#include <Storages/DeltaMerge/Delta/DeltaPackDeleteRange.h>
#include <Storages/DeltaMerge/Delta/DeltaPackFile.h>

namespace DB
{
namespace DM
{

struct Pack_V2
{
    UInt64      rows  = 0;
    UInt64      bytes = 0;
    BlockPtr    schema;
    RowKeyRange delete_range;
    PageId      data_page_id = 0;

    bool isDeleteRange() const { return !delete_range.none(); }
};
using Pack_V2Ptr = std::shared_ptr<Pack_V2>;
using Packs_V2   = std::vector<Pack_V2Ptr>;

inline DeltaPacks transform_V2_to_V3(const Packs_V2 & packs_v2)
{
    DeltaPacks packs_v3;
    for (auto & p : packs_v2)
    {
        DeltaPackPtr p_v3 = p->isDeleteRange() ? std::make_shared<DeltaPackDeleteRange>(std::move(p->delete_range))
                                               : DeltaPackBlock::createPackWithDataPage(p->schema, p->rows, p->bytes, p->data_page_id);
        packs_v3.push_back(p_v3);
    }
    return packs_v3;
}

inline Packs_V2 transformSaved_V3_to_V2(const DeltaPacks & packs_v3)
{
    Packs_V2 packs_v2;
    for (auto & p : packs_v3)
    {
        if (!p->isSaved())
            break;

        auto p_v2 = new Pack_V2();

        if (auto dp_delete = p->tryToDeleteRange(); dp_delete)
        {
            p_v2->delete_range = dp_delete->getDeleteRange();
        }
        else if (auto dp_block = p->tryToBlock(); dp_block)
        {
            p_v2->rows         = dp_block->getRows();
            p_v2->bytes        = dp_block->getBytes();
            p_v2->schema       = dp_block->getSchema();
            p_v2->data_page_id = dp_block->getDataPageId();
        }
        else
        {
            throw Exception("Unexpected delta pack type", ErrorCodes::LOGICAL_ERROR);
        }

        packs_v2.push_back(std::shared_ptr<Pack_V2>(p_v2));
    }
    return packs_v2;
}

inline void serializePack_V2(const Pack_V2 & pack, const BlockPtr & schema, WriteBuffer & buf)
{
    writeIntBinary(pack.rows, buf);
    writeIntBinary(pack.bytes, buf);
    pack.delete_range.serialize(buf);
    writeIntBinary(pack.data_page_id, buf);
    if (schema)
    {
        writeIntBinary((UInt32)schema->columns(), buf);
        for (auto & col : *pack.schema)
        {
            writeIntBinary(col.column_id, buf);
            writeStringBinary(col.name, buf);
            writeStringBinary(col.type->getName(), buf);
        }
    }
    else
    {
        writeIntBinary((UInt32)0, buf);
    }
}

void serializeSavedPacks_V2(WriteBuffer & buf, const Packs_V2 & packs)
{
    writeIntBinary(packs.size(), buf);
    BlockPtr last_schema;
    for (auto & pack : packs)
    {
        // Do not encode the schema if it is the same as previous one.
        if (pack->isDeleteRange())
            serializePack_V2(*pack, nullptr, buf);
        else
        {
            if (unlikely(!pack->schema))
                throw Exception("A data pack without schema", ErrorCodes::LOGICAL_ERROR);
            if (pack->schema != last_schema)
            {
                serializePack_V2(*pack, pack->schema, buf);
                last_schema = pack->schema;
            }
            else
            {
                serializePack_V2(*pack, nullptr, buf);
            }
        }
    }
}

void serializeSavedPacks_v2(WriteBuffer & buf, const DeltaPacks & packs)
{
    serializeSavedPacks_V2(buf, transformSaved_V3_to_V2(packs));
}

inline Pack_V2Ptr deserializePack_V2(ReadBuffer & buf, UInt64 version)
{
    auto pack = std::make_shared<Pack_V2>();
    readIntBinary(pack->rows, buf);
    readIntBinary(pack->bytes, buf);
    switch (version)
    {
    case DeltaFormat::V1: {
        HandleRange range;
        readPODBinary(range, buf);
        pack->delete_range = RowKeyRange::fromHandleRange(range);
        break;
    }
    case DeltaFormat::V2:
        pack->delete_range = RowKeyRange::deserialize(buf);
        break;
    default:
        throw Exception("Unexpected version: " + DB::toString(version), ErrorCodes::LOGICAL_ERROR);
    }

    readIntBinary(pack->data_page_id, buf);

    pack->schema = deserializeSchema(buf);
    return pack;
}

DeltaPacks deserializePacks_V2(ReadBuffer & buf, UInt64 version)
{
    size_t size;
    readIntBinary(size, buf);
    Packs_V2 packs;
    BlockPtr last_schema;
    for (size_t i = 0; i < (size_t)size; ++i)
    {
        auto pack = deserializePack_V2(buf, version);
        if (!pack->isDeleteRange())
        {
            if (!pack->schema)
                pack->schema = last_schema;
            else
                last_schema = pack->schema;
        }
        packs.push_back(pack);
    }
    return transform_V2_to_V3(packs);
}

} // namespace DM
} // namespace DB