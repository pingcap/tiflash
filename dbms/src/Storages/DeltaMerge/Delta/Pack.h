//#pragma once
//
//#include <Columns/IColumn.h>
//#include <Core/Block.h>
//#include <Core/Types.h>
//#include <Storages/DeltaMerge/DeltaValueSpace.h>
//#include <Storages/Page/PageStorage.h>
//
//namespace DB
//{
//
//class MemoryWriteBuffer;
//
//namespace DM
//{
//
//static constexpr size_t PACK_SERIALIZE_BUFFER_SIZE = 65536;
//
//void serializeColumn(MemoryWriteBuffer & buf, const IColumn & column, const DataTypePtr & type, size_t offset, size_t limit, bool compress);
//
//void  serializeSavedPacks(WriteBuffer & buf, const Packs & packs);
//Packs deserializePacks(ReadBuffer & buf);
//
//// Debugging string
//String packsToString(const Packs & packs);
//
//// Read a block from cache / disk according to `pack->schema`. Only used by Compact Delta.
//Block readPackFromCache(const PackPtr & pack);
//Block readPackFromDisk(const PackPtr & pack, const PageReader & page_reader);
//
//// Read a block of columns in `column_defines` from cache / disk,
//// if `pack->schema` is not match with `column_defines`, take good care of DDL cast
//Columns readPackFromCache(const PackPtr & pack, const ColumnDefines & column_defines, size_t col_start, size_t col_end);
//Columns readPackFromDisk(const PackPtr &       pack, //
//                         const PageReader &    page_reader,
//                         const ColumnDefines & column_defines,
//                         size_t                col_start,
//                         size_t                col_end);
//
//} // namespace DM
//} // namespace DB
