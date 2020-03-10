#pragma once

#include <Columns/IColumn.h>
#include <Core/Block.h>
#include <Core/Types.h>
#include <Storages/DeltaMerge/DeltaValueSpace.h>

namespace DB::DM
{

using BlockPtr = DeltaValueSpace::BlockPtr;

static constexpr size_t PACK_SERIALIZE_BUFFER_SIZE = 65536;

void serializeColumn(MemoryWriteBuffer & buf, const IColumn & column, const DataTypePtr & type, size_t offset, size_t limit, bool compress);

void  serializeSavedPacks(WriteBuffer & buf, const Packs & packs);
Packs deserializePacks(ReadBuffer & buf);

String  packsToString(const Packs & packs);
Block   readPackFromCache(const PackPtr & pack);
Columns readPackFromCache(const PackPtr & pack, const ColumnDefines & column_defines, size_t col_start, size_t col_end);
Block   readPackFromDisk(const PackPtr & pack, const PageReader & page_reader);
Columns readPackFromDisk(const PackPtr &       pack, //
                         const PageReader &    page_reader,
                         const ColumnDefines & column_defines,
                         size_t                col_start,
                         size_t                col_end);

} // namespace DB::DM
