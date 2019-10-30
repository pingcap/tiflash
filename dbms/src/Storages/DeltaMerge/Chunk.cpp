#include <cstring>

#include <Storages/DeltaMerge/Chunk.h>

#include <DataTypes/isSupportedDataTypeCast.h>
#include <Functions/FunctionHelpers.h>
#include <IO/CompressedReadBuffer.h>
#include <IO/CompressedWriteBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB
{
namespace DM
{

const Chunk::Version Chunk::CURRENT_VERSION = 1;

void Chunk::serialize(WriteBuffer & buf) const
{
    writeVarUInt(Chunk::CURRENT_VERSION, buf); // Add binary version

    writeIntBinary(handle_start, buf);
    writeIntBinary(handle_end, buf);
    writePODBinary(is_delete_range, buf);
    writeIntBinary((UInt64)columns.size(), buf);
    for (const auto & [col_id, d] : columns)
    {
        writeIntBinary(col_id, buf);
        writeIntBinary(d.page_id, buf);
        writeIntBinary(d.rows, buf);
        writeIntBinary(d.bytes, buf);
        writeStringBinary(d.type->getName(), buf);
        if (d.minmax)
        {
            writePODBinary(true, buf);
            d.minmax->write(*d.type, buf);
        }
        else
        {
            writePODBinary(false, buf);
        }
    }
}

Chunk Chunk::deserialize(ReadBuffer & buf)
{
    // Check binary version
    Chunk::Version chunk_batch_version;
    readVarUInt(chunk_batch_version, buf);
    if (chunk_batch_version != Chunk::CURRENT_VERSION)
        throw Exception("Chunk binary version not match: " + DB::toString(chunk_batch_version), ErrorCodes::LOGICAL_ERROR);

    Handle start, end;
    readIntBinary(start, buf);
    readIntBinary(end, buf);

    Chunk chunk(start, end);

    readPODBinary(chunk.is_delete_range, buf);
    UInt64 col_size;
    readIntBinary(col_size, buf);
    chunk.columns.reserve(col_size);
    for (UInt64 ci = 0; ci < col_size; ++ci)
    {
        ColumnMeta d;
        String     type;
        readIntBinary(d.col_id, buf);
        readIntBinary(d.page_id, buf);
        readIntBinary(d.rows, buf);
        readIntBinary(d.bytes, buf);
        readStringBinary(type, buf);
        d.type = DataTypeFactory::instance().get(type);
        bool has_minmax;
        readPODBinary(has_minmax, buf);
        if (has_minmax)
            d.minmax = MinMaxIndex::read(*d.type, buf);

        chunk.columns.emplace(d.col_id, d);

        if (chunk.rows != 0 && chunk.rows != d.rows)
            throw Exception("Rows not match");
        else
            chunk.rows = d.rows;
    }
    return chunk;
}

Chunk createRefChunk(const Chunk & chunk, const GenPageId & gen_data_page_id, WriteBatch & wb)
{
    if (chunk.isDeleteRange())
        return Chunk(chunk.getDeleteRange());

    auto [handle_first, handle_end] = chunk.getHandleFirstLast();
    Chunk ref_chunk(handle_first, handle_end);
    for (auto && [col_id, col_meta] : chunk.getMetas())
    {
        ColumnMeta m;

        m.col_id  = col_id;
        m.page_id = gen_data_page_id();
        m.rows    = col_meta.rows;
        m.bytes   = col_meta.bytes;
        m.type    = col_meta.type;
        m.minmax  = col_meta.minmax;

        wb.putRefPage(m.page_id, col_meta.page_id);
        ref_chunk.insert(m);
    }
    return ref_chunk;
}

Chunks createRefChunks(const Chunks & chunks, const GenPageId & gen_data_page_id, WriteBatch & wb)
{
    Chunks ref_chunks;
    ref_chunks.reserve(chunks.size());
    for (auto & chunk : chunks)
        ref_chunks.push_back(createRefChunk(chunk, gen_data_page_id, wb));
    return ref_chunks;
}

void serializeChunks(
    WriteBuffer & buf, Chunks::const_iterator begin, Chunks::const_iterator end, const Chunk * extra1, const Chunk * extra2)
{
    auto size = (UInt64)(end - begin);
    if (extra1)
        ++size;
    if (extra2)
        ++size;
    writeIntBinary(size, buf);

    for (; begin != end; ++begin)
        (*begin).serialize(buf);
    if (extra1)
        extra1->serialize(buf);
    if (extra2)
        extra2->serialize(buf);
}

void serializeChunks(WriteBuffer & buf, Chunks::const_iterator begin, Chunks ::const_iterator end, const Chunks & extra_chunks)
{
    auto size = (UInt64)(end - begin) + extra_chunks.size();
    writeIntBinary(size, buf);

    for (; begin != end; ++begin)
        (*begin).serialize(buf);
    for (auto & chunk : extra_chunks)
        chunk.serialize(buf);
}

Chunks deserializeChunks(ReadBuffer & buf)
{
    Chunks chunks;
    UInt64 size;
    readIntBinary(size, buf);
    for (UInt64 i = 0; i < size; ++i)
        chunks.push_back(Chunk::deserialize(buf));
    return chunks;
}

using BufferAndSize = std::pair<ReadBufferPtr, size_t>;
BufferAndSize serializeColumn(const IColumn & column, const DataTypePtr & type, size_t offset, size_t num, bool compress)
{
    MemoryWriteBuffer plain;
    CompressionMethod method = compress ? CompressionMethod::LZ4 : CompressionMethod::NONE;

    CompressedWriteBuffer compressed(plain, CompressionSettings(method));
    type->serializeBinaryBulkWithMultipleStreams(column, //
                                                 [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                 offset,
                                                 num,
                                                 true,
                                                 {});
    compressed.next();

    auto data_size = plain.count();
    return {plain.tryGetReadBuffer(), data_size};
}

Chunk prepareChunkDataWrite(const DMContext & dm_context, const GenPageId & gen_data_page_id, WriteBatch & wb, const Block & block)
{
    auto & handle_col_data = getColumnVectorData<Handle>(block, block.getPositionByName(dm_context.handle_column.name));
    Chunk  chunk(handle_col_data[0], handle_col_data[handle_col_data.size() - 1]);
    for (const auto & col_define : dm_context.store_columns)
    {
        auto            col_id = col_define.id;
        const IColumn & column = *(block.getByName(col_define.name).column);
        auto [buf, size]       = serializeColumn(column, col_define.type, 0, column.size(), !dm_context.not_compress.count(col_id));

        ColumnMeta d;
        d.col_id  = col_id;
        d.page_id = gen_data_page_id();
        d.rows    = column.size();
        d.bytes   = size;
        d.type    = col_define.type;
        d.minmax  = std::make_shared<MinMaxIndex>(
            *col_define.type, column, static_cast<const ColumnVector<UInt8> &>(*block.getByName(TAG_COLUMN_NAME).column), 0, column.size());

        wb.putPage(d.page_id, 0, buf, size);
        chunk.insert(d);
    }

    return chunk;
}

void deserializeColumn(IColumn & column, const ColumnMeta & meta, const Page & page, size_t rows_limit)
{
    ReadBufferFromMemory buf(page.data.begin(), page.data.size());
    CompressedReadBuffer compressed(buf);
    meta.type->deserializeBinaryBulkWithMultipleStreams(column, //
                                                        [&](const IDataType::SubstreamPath &) { return &compressed; },
                                                        rows_limit,
                                                        (double)(page.data.size()) / meta.rows,
                                                        true,
                                                        {});
}

void readChunkData(MutableColumns &      columns,
                   const ColumnDefines & column_defines,
                   const Chunk &         chunk,
                   const PageReader &    page_reader,
                   size_t                rows_offset,
                   size_t                rows_limit)
{
    assert(!chunk.isDeleteRange());

    std::unordered_map<PageId, size_t> page_to_index;
    PageIds                            page_ids;
    page_ids.reserve(column_defines.size());
    for (size_t index = 0; index < column_defines.size(); ++index)
    {
        const auto & define = column_defines[index];
        if (chunk.hasColumn(define.id))
        {
            // Read chunk's data from PageStorage later
            auto page_id = chunk.getColumn(define.id).page_id;
            page_ids.push_back(page_id);
            page_to_index[page_id] = index;
        }
        else
        {
            // New column after ddl is not exist in chunk's meta, fill with default value
            IColumn & col = *columns[index];

            // Read default value from `define.default_value`
            ColumnPtr tmp_col;
            if (define.default_value.isNull())
            {
                tmp_col = define.type->createColumnConstWithDefaultValue(rows_limit);
            }
            // TODO: `define.default_value` may be not matched with `define.type`
            // For example: ... ADD COLUMN f32 Float32 '1.23'
            // After parsing and interpreting, we will get a `type` Float32,
            // while `default_value` will be inferred as String.
            // So we should do some process before reading value or during applying alter to guarantee the correctness.
            else if (define.type->equals(*DataTypeFactory::instance().get("Float32"))
                     || define.type->equals(*DataTypeFactory::instance().get("Float64")))
            {
                Float64 real;
                auto    dec32  = DecimalField(Decimal32(), 0);
                auto    dec64  = DecimalField(Decimal64(), 0);
                auto    dec128 = DecimalField(Decimal128(), 0);
                auto    dec256 = DecimalField(Decimal256(), 0);
                if (define.default_value.tryGet(dec32))
                {
                    real = static_cast<Float64>(dec32);
                }
                else if (define.default_value.tryGet(dec64))
                {
                    real = static_cast<Float64>(dec64);
                }
                else if (define.default_value.tryGet(dec128))
                {
                    real = static_cast<Float64>(dec128);
                }
                else if (define.default_value.tryGet(dec256))
                {
                    real = static_cast<Float64>(dec256);
                }
                else
                {
                    throw Exception("Unsupported literal for default value", ErrorCodes::NOT_IMPLEMENTED);
                }
                tmp_col = define.type->createColumnConst(rows_limit, Field(real));
            }
            else if (define.type->equals(*DataTypeFactory::instance().get("DateTime")))
            {
                auto                 date = safeGet<String>(define.default_value);
                time_t               time = 0;
                ReadBufferFromMemory buf(date.data(), date.size());
                readDateTimeText(time, buf);
                tmp_col = define.type->createColumnConst(rows_limit, Field(UInt64(time)));
            }
            else if (std::strcmp(define.type->getFamilyName(), "Decimal") == 0)
            {
                Int64  value    = 0;
                Int128 value128 = 0;
                Int256 value256 = 0;
                UInt32 scale    = 0;
                {
                    auto dec32  = DecimalField(Decimal32(), 0);
                    auto dec64  = DecimalField(Decimal64(), 0);
                    auto dec128 = DecimalField(Decimal128(), 0);
                    auto dec256 = DecimalField(Decimal256(), 0);
                    if (define.default_value.tryGet(dec32))
                    {
                        value = dec32.getValue().value;
                        scale = dec32.getScale();
                    }
                    else if (define.default_value.tryGet(dec64))
                    {
                        value = dec64.getValue().value;
                        scale = dec64.getScale();
                    }
                    else if (define.default_value.tryGet(dec128))
                    {
                        value128 = dec128.getValue().value;
                        scale = dec128.getScale();
                    }
                    else if (define.default_value.tryGet(dec256))
                    {
                        value256 = dec256.getValue().value;
                        scale = dec256.getScale();
                    }
                }

                if (define.type->getTypeId() == TypeIndex::Decimal32)
                {
                    auto dec = DecimalField<Decimal32>(value, scale);
                    tmp_col  = define.type->createColumnConst(rows_limit, toField(dec));
                }
                else if (define.type->getTypeId() == TypeIndex::Decimal64)
                {
                    auto dec = DecimalField<Decimal64>(value, scale);
                    tmp_col  = define.type->createColumnConst(rows_limit, toField(dec));
                }
                else if (define.type->getTypeId() == TypeIndex::Decimal128)
                {
                    auto dec = DecimalField<Decimal128>(value128, scale);
                    tmp_col  = define.type->createColumnConst(rows_limit, toField(dec));
                }
                else if (define.type->getTypeId() == TypeIndex::Decimal256)
                {
                    auto dec = DecimalField<Decimal256>(value256, scale);
                    tmp_col  = define.type->createColumnConst(rows_limit, toField(dec));
                }
                else
                {
                    throw Exception("Unsupported literal for default value", ErrorCodes::NOT_IMPLEMENTED);
                }
            }
            else
            {
                tmp_col = define.type->createColumnConst(rows_limit, define.default_value);
            }
            tmp_col = tmp_col->convertToFullColumnIfConst();

            col.insertRangeFrom(*tmp_col, 0, rows_limit);
        }
    }

    PageHandler page_handler = [&](PageId page_id, const Page & page) {
        size_t               index       = page_to_index[page_id];
        IColumn &            col         = *columns[index];
        const ColumnDefine & read_define = column_defines[index];
        const ColumnMeta &   disk_meta   = chunk.getColumn(read_define.id);

        // define.type is current type at memory
        // meta.type is the type at disk (maybe different from define.type)

        if (read_define.type->equals(*disk_meta.type))
        {
            if (rows_offset == 0)
            {
                deserializeColumn(col, disk_meta, page, rows_limit);
            }
            else
            {
                MutableColumnPtr tmp_col = read_define.type->createColumn();
                deserializeColumn(*tmp_col, disk_meta, page, rows_offset + rows_limit);
                col.insertRangeFrom(*tmp_col, rows_offset, rows_limit);
            }
        }
        else
        {
#ifndef NDEBUG
            const auto && [first, last] = chunk.getHandleFirstLast();
            const String disk_col_str   = "col{name:" + DB::toString(read_define.name) + ",id:" + DB::toString(disk_meta.col_id)
                + ",type:" + disk_meta.type->getName() + "]";
            LOG_TRACE(&Poco::Logger::get("Chunk"),
                      "Reading chunk[" + DB::toString(first) + "-" + DB::toString(last) + "] " + disk_col_str + " as type "
                          + read_define.type->getName());
#endif

            // sanity check
            if (unlikely(!isSupportedDataTypeCast(disk_meta.type, read_define.type)))
            {
                throw Exception("Reading mismatch data type chunk. Cast from " + disk_meta.type->getName() + " to "
                                    + read_define.type->getName() + " is NOT supported!",
                                ErrorCodes::NOT_IMPLEMENTED);
            }

            // Read from disk according to chunk meta
            MutableColumnPtr disk_col = disk_meta.type->createColumn();
            deserializeColumn(*disk_col, disk_meta, page, rows_offset + rows_limit);

            // Cast column's data from DataType in disk to what we need now
            castColumnAccordingToColumnDefine(disk_meta.type, disk_col->getPtr(), read_define, col.getPtr(), rows_offset, rows_limit);
        }
    };
    page_reader.read(page_ids, page_handler);
}


Block readChunk(const Chunk & chunk, const ColumnDefines & read_column_defines, const PageReader & page_reader)
{
    if (read_column_defines.empty())
        return {};

    MutableColumns columns;
    for (const auto & define : read_column_defines)
    {
        columns.emplace_back(define.type->createColumn());
        columns.back()->reserve(chunk.getRows());
    }

    if (chunk.getRows())
    {
        // Read from storage
        readChunkData(columns, read_column_defines, chunk, page_reader, 0, chunk.getRows());
    }

    Block res;
    for (size_t index = 0; index < read_column_defines.size(); ++index)
    {
        const ColumnDefine &  define = read_column_defines[index];
        ColumnWithTypeAndName col(std::move(columns[index]), define.type, define.name, define.id);
        res.insert(std::move(col));
    }
    return res;
}

//==========================================================================================
// Functions for casting column data when disk data type mismatch with read data type.
//==========================================================================================

namespace
{

/// some helper functions for casting column data type

bool castNonNullNumericColumn(const DataTypePtr &  disk_type_not_null_,
                              const ColumnPtr &    disk_col_not_null,
                              const ColumnDefine & read_define,
                              const ColumnPtr &    null_map,
                              MutableColumnPtr &   memory_col_not_null,
                              size_t               rows_offset,
                              size_t               rows_limit);


template <typename TypeFrom, typename TypeTo>
void insertRangeFromWithNumericTypeCast(const ColumnPtr &    from_col, //
                                        const ColumnPtr &    null_map,
                                        const ColumnDefine & read_define,
                                        MutableColumnPtr &   to_col,
                                        size_t               rows_offset,
                                        size_t               rows_limit);

} // namespace

void castColumnAccordingToColumnDefine(const DataTypePtr &  disk_type,
                                       const ColumnPtr &    disk_col,
                                       const ColumnDefine & read_define,
                                       MutableColumnPtr     memory_col,
                                       size_t               rows_offset,
                                       size_t               rows_limit)
{
#if 0
    // A simple but awful version using Field.
    for (size_t i = 0; i < disk_col->size(); ++i)
    {
        Field f = (*disk_col)[i];
        if (f.getType() == Field::Types::Null)
            memory_col->insertDefault();
        else
            memory_col->insert(std::move(f));
    }
#else
    const DataTypePtr & read_type = read_define.type;

    // Unwrap nullable(what)
    ColumnPtr        disk_col_not_null;
    MutableColumnPtr memory_col_not_null;
    ColumnPtr        null_map;
    DataTypePtr      disk_type_not_null = disk_type;
    DataTypePtr      read_type_not_null = read_type;
    if (disk_type->isNullable() && read_type->isNullable())
    {
        // nullable -> nullable, copy null map
        const auto & disk_nullable_col   = typeid_cast<const ColumnNullable &>(*disk_col);
        const auto & disk_null_map       = disk_nullable_col.getNullMapData();
        auto &       memory_nullable_col = typeid_cast<ColumnNullable &>(*memory_col);
        auto &       memory_null_map     = memory_nullable_col.getNullMapData();
        memory_null_map.insert(disk_null_map.begin(), disk_null_map.end());

        disk_col_not_null   = disk_nullable_col.getNestedColumnPtr();
        memory_col_not_null = memory_nullable_col.getNestedColumn().getPtr();

        const auto * type_nullable = typeid_cast<const DataTypeNullable *>(disk_type.get());
        disk_type_not_null         = type_nullable->getNestedType();
        type_nullable              = typeid_cast<const DataTypeNullable *>(read_type.get());
        read_type_not_null         = type_nullable->getNestedType();
    }
    else if (!disk_type->isNullable() && read_type->isNullable())
    {
        // not null -> nullable, set null map to all not null
        auto & memory_nullable_col = typeid_cast<ColumnNullable &>(*memory_col);
        auto & nullmap_data        = memory_nullable_col.getNullMapData();
        nullmap_data.resize_fill(rows_offset + rows_limit, 0);

        disk_col_not_null   = disk_col;
        memory_col_not_null = memory_nullable_col.getNestedColumn().getPtr();

        const auto * type_nullable = typeid_cast<const DataTypeNullable *>(read_type.get());
        read_type_not_null         = type_nullable->getNestedType();
    }
    else if (disk_type->isNullable() && !read_type->isNullable())
    {
        // nullable -> not null, fill "NULL" values with default value later
        const auto & disk_nullable_col = typeid_cast<const ColumnNullable &>(*disk_col);
        null_map                       = disk_nullable_col.getNullMapColumnPtr();
        disk_col_not_null              = disk_nullable_col.getNestedColumnPtr();
        memory_col_not_null            = std::move(memory_col);

        const auto * type_nullable = typeid_cast<const DataTypeNullable *>(disk_type.get());
        disk_type_not_null         = type_nullable->getNestedType();
    }
    else
    {
        // not null -> not null
        disk_col_not_null   = disk_col;
        memory_col_not_null = std::move(memory_col);
    }

    assert(memory_col_not_null != nullptr);
    assert(disk_col_not_null != nullptr);
    assert(read_type_not_null != nullptr);
    assert(disk_type_not_null != nullptr);

    ColumnDefine read_define_not_null(read_define);
    read_define_not_null.type = read_type_not_null;
    if (disk_type_not_null->equals(*read_type_not_null))
    {
        // just change from nullable -> not null / not null -> nullable
        memory_col_not_null->insertRangeFrom(*disk_col_not_null, rows_offset, rows_limit);

        if (null_map)
        {
            /// We are applying cast from nullable to not null, scan to fill "NULL" with default value

            for (size_t i = 0; i < rows_limit; ++i)
            {
                if (unlikely(null_map->getInt(i) != 0))
                {
                    // `from_col[i]` is "NULL", fill `to_col[rows_offset + i]` with default value
                    // TiDB/MySQL don't support this, should not call here.
                    throw Exception("Reading mismatch data type chunk. Cast from " + disk_type->getName() + " to " + read_type->getName()
                                        + " with \"NULL\" value is NOT supported!",
                                    ErrorCodes::NOT_IMPLEMENTED);
                }
            }
        }
    }
    else if (!castNonNullNumericColumn(
                 disk_type_not_null, disk_col_not_null, read_define_not_null, null_map, memory_col_not_null, rows_offset, rows_limit))
    {
        throw Exception("Reading mismatch data type chunk. Cast and assign from " + disk_type->getName() + " to " + read_type->getName()
                            + " is NOT supported!",
                        ErrorCodes::NOT_IMPLEMENTED);
    }
#endif
}

namespace
{
bool castNonNullNumericColumn(const DataTypePtr &  disk_type_not_null_,
                              const ColumnPtr &    disk_col_not_null,
                              const ColumnDefine & read_define,
                              const ColumnPtr &    null_map,
                              MutableColumnPtr &   memory_col_not_null,
                              size_t               rows_offset,
                              size_t               rows_limit)
{
    /// Caller should ensure that type is not nullable
    assert(disk_type_not_null_ != nullptr);
    assert(disk_col_not_null != nullptr);
    assert(read_define.type != nullptr);
    assert(memory_col_not_null != nullptr);

    const IDataType * disk_type_not_null = disk_type_not_null_.get();
    const IDataType * read_type_not_null = read_define.type.get();

    /// Caller should ensure nullable is unwrapped
    assert(!disk_type_not_null->isNullable());
    assert(!read_type_not_null->isNullable());

    /// Caller should ensure that dist_type != read_type
    assert(!disk_type_not_null->equals(*read_type_not_null));

    if (checkDataType<DataTypeUInt32>(disk_type_not_null))
    {
        using FromType = UInt32;
        if (checkDataType<DataTypeUInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt64>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeInt32>(disk_type_not_null))
    {
        using FromType = Int32;
        if (checkDataType<DataTypeInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int64>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeUInt16>(disk_type_not_null))
    {
        using FromType = UInt16;
        if (checkDataType<DataTypeUInt32>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt32>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeUInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt64>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeInt16>(disk_type_not_null))
    {
        using FromType = Int16;
        if (checkDataType<DataTypeInt32>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int32>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int64>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeUInt8>(disk_type_not_null))
    {
        using FromType = UInt8;
        if (checkDataType<DataTypeUInt32>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt32>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeUInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt64>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeUInt16>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, UInt16>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
    }
    else if (checkDataType<DataTypeInt8>(disk_type_not_null))
    {
        using FromType = Int8;
        if (checkDataType<DataTypeInt32>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int32>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeInt64>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int64>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
        else if (checkDataType<DataTypeInt16>(read_type_not_null))
        {
            insertRangeFromWithNumericTypeCast<FromType, Int16>(
                disk_col_not_null, null_map, read_define, memory_col_not_null, rows_offset, rows_limit);
            return true;
        }
    }

    // else is not support
    return false;
}

template <typename TypeFrom, typename TypeTo>
void insertRangeFromWithNumericTypeCast(const ColumnPtr &    from_col, //
                                        const ColumnPtr &    null_map,
                                        const ColumnDefine & read_define,
                                        MutableColumnPtr &   to_col,
                                        size_t               rows_offset,
                                        size_t               rows_limit)
{
    // Caller should ensure that both from_col / to_col
    // * is numeric
    // * no nullable wrapper
    // * both signed or unsigned
    static_assert(std::is_integral_v<TypeFrom>);
    static_assert(std::is_integral_v<TypeTo>);
    constexpr bool is_both_signed_or_unsigned = !(std::is_unsigned_v<TypeFrom> ^ std::is_unsigned_v<TypeTo>);
    static_assert(is_both_signed_or_unsigned);
    assert(from_col != nullptr);
    assert(to_col != nullptr);
    assert(from_col->isNumeric());
    assert(to_col->isNumeric());
    assert(!from_col->isColumnNullable());
    assert(!to_col->isColumnNullable());
    assert(!from_col->isColumnConst());
    assert(!to_col->isColumnConst());

    // Something like `insertRangeFrom(from_col, rows_offset, rows_limit)` with static_cast
    const PaddedPODArray<TypeFrom> & from_array   = toColumnVectorData<TypeFrom>(from_col);
    PaddedPODArray<TypeTo> *         to_array_ptr = toMutableColumnVectorDataPtr<TypeTo>(to_col);
    to_array_ptr->reserve(rows_limit);
    for (size_t i = 0; i < rows_limit; ++i)
    {
        (*to_array_ptr).emplace_back(static_cast<TypeTo>(from_array[rows_offset + i]));
    }

    if (unlikely(null_map))
    {
        /// We are applying cast from nullable to not null, scan to fill "NULL" with default value

        TypeTo default_value = 0; // if read_define.default_value is empty, fill with 0
        if (read_define.default_value.isNull())
        {
            // Do nothing
        }
        else if (read_define.default_value.getType() == Field::Types::Int64)
        {
            default_value = read_define.default_value.safeGet<Int64>();
        }
        else if (read_define.default_value.getType() == Field::Types::UInt64)
        {
            default_value = read_define.default_value.safeGet<UInt64>();
        }
        else
        {
            throw Exception("Invalid column value type", ErrorCodes::BAD_ARGUMENTS);
        }

        const size_t to_offset_before_inserted = to_array_ptr->size() - rows_limit;

        for (size_t i = 0; i < rows_limit; ++i)
        {
            const size_t to_offset = to_offset_before_inserted + i;
            if (null_map->getInt(rows_offset + i) != 0)
            {
                // `from_col[rows_offset + i]` is "NULL", fill `to_col[x]` with default value
                (*to_array_ptr)[to_offset] = static_cast<TypeTo>(default_value);
            }
        }
    }
}


} // namespace

} // namespace DM
} // namespace DB
