#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <Storages/DeltaMerge/Delta/DeltaPack.h>
#include <Storages/DeltaMerge/Delta/DeltaPackBlock.h>
#include <Storages/DeltaMerge/Delta/DeltaPackDeleteRange.h>
#include <Storages/DeltaMerge/Delta/DeltaPackFile.h>
#include <Storages/DeltaMerge/Delta/DeltaValueSpace.h>
#include <Storages/DeltaMerge/RowKeyFilter.h>

namespace DB
{
namespace DM
{

DeltaPackBlock * DeltaPack::tryToBlock()
{
    return !isBlock() ? nullptr : static_cast<DeltaPackBlock *>(this);
}

DeltaPackFile * DeltaPack::tryToFile()
{
    return !isFile() ? nullptr : static_cast<DeltaPackFile *>(this);
}

DeltaPackDeleteRange * DeltaPack::tryToDeleteRange()
{
    return !isDeleteRange() ? nullptr : static_cast<DeltaPackDeleteRange *>(this);
}

void serializeSavedPacks(WriteBuffer & buf, const DeltaPacks & packs)
{
    writeIntBinary(STORAGE_FORMAT_CURRENT.delta, buf); // Add binary version
    switch (STORAGE_FORMAT_CURRENT.delta)
    {
        // V1 and V2 share the same serializer.
    case DeltaFormat::V1:
    case DeltaFormat::V2:
        serializeSavedPacks_v2(buf, packs);
        break;
    case DeltaFormat::V3:
        serializeSavedPacks_V3(buf, packs);
        break;
    default:
        throw Exception("Unexpected delta value version: " + DB::toString(STORAGE_FORMAT_CURRENT.delta), ErrorCodes::LOGICAL_ERROR);
    }
}

DeltaPacks deserializePacks(DMContext & context, const RowKeyRange & segment_range, ReadBuffer & buf)
{
    // Check binary version
    DeltaFormat::Version version;
    readIntBinary(version, buf);

    DeltaPacks packs;
    switch (version)
    {
    // V1 and V2 share the same deserializer.
    case DeltaFormat::V1:
    case DeltaFormat::V2:
        packs = deserializePacks_V2(buf, version);
        break;
    case DeltaFormat::V3:
        packs = deserializePacks_V3(context, segment_range, buf, version);
        break;
    default:
        throw Exception("Unexpected delta value version: " + DB::toString(version) + ", latest version: " + DB::toString(DeltaFormat::V3),
                        ErrorCodes::LOGICAL_ERROR);
    }
    for (auto & p : packs)
        p->setSaved();
    return packs;
}


String packsToString(const DeltaPacks & packs)
{
    String packs_info = "[";
    for (auto & p : packs)
    {
        if (p->isBlock())
            packs_info += "B_" + DB::toString(p->getRows());
        else if (p->isFile())
            packs_info += "F_" + DB::toString(p->getRows());
        else if (auto dp_delete = p->tryToDeleteRange(); dp_delete)
            packs_info += "D_" + dp_delete->getDeleteRange().toString();
        packs_info += (p->isSaved() ? "_S," : "_N,");
    }
    if (!packs.empty())
        packs_info.erase(packs_info.size() - 1);
    packs_info += "]";
    return packs_info;
}

} // namespace DM
} // namespace DB
