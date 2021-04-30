#include <Storages/DeltaMerge/Delta/DeltaPack.h>
#include <Storages/DeltaMerge/Delta/DeltaPackBlock.h>
#include <Storages/DeltaMerge/Delta/DeltaPackDeleteRange.h>
#include <Storages/DeltaMerge/Delta/DeltaPackFile.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>

namespace DB
{
namespace DM
{

void serializeSavedPacks_V3(WriteBuffer & buf, const DeltaPacks & packs)
{
    size_t saved_packs = std::find_if(packs.begin(), packs.end(), [](const DeltaPackPtr & p) { return !p->isSaved(); }) - packs.begin();

    writeIntBinary(saved_packs, buf);
    BlockPtr last_schema;

    for (auto & pack : packs)
    {
        if (!pack->isSaved())
            break;
        // Do not encode the schema if it is the same as previous one.
        writeIntBinary(pack->getType(), buf);

        switch (pack->getType())
        {
        case DeltaPack::Type::DELETE_RANGE: {
            pack->serializeMetadata(buf, false);
            break;
        }
        case DeltaPack::Type::FILE: {
            pack->serializeMetadata(buf, false);
            break;
        }
        case DeltaPack::Type::BLOCK: {
            auto dp_block   = pack->tryToBlock();
            auto cur_schema = dp_block->getSchema();
            if (unlikely(!cur_schema))
                throw Exception("A data pack without schema: " + pack->toString(), ErrorCodes::LOGICAL_ERROR);

            bool save_schema = cur_schema != last_schema;
            pack->serializeMetadata(buf, save_schema);
            break;
        }
        default:
            throw Exception("Unexpected type", ErrorCodes::LOGICAL_ERROR);
        }
    }
}

DeltaPacks deserializePacks_V3(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf, UInt64 /*version*/)
{
    size_t pack_count;
    readIntBinary(pack_count, buf);
    DeltaPacks packs;
    BlockPtr   last_schema;
    for (size_t i = 0; i < pack_count; ++i)
    {
        std::underlying_type<DeltaPack::Type>::type pack_type;
        readIntBinary(pack_type, buf);
        DeltaPackPtr pack;
        switch (pack_type)
        {
        case DeltaPack::Type::DELETE_RANGE:
            pack = DeltaPackDeleteRange::deserializeMetadata(buf);
            break;
        case DeltaPack::Type::BLOCK: {
            std::tie(pack, last_schema) = DeltaPackBlock::deserializeMetadata(buf, last_schema);
            break;
        }
        case DeltaPack::Type::FILE: {
            pack = DeltaPackFile::deserializeMetadata(context, segment_range, buf);
            break;
        }
        default:
            throw Exception("Unexpected pack type: " + DB::toString(pack_type), ErrorCodes::LOGICAL_ERROR);
        }
        packs.emplace_back(std::move(pack));
    }
    return packs;
}

} // namespace DM
} // namespace DB
