// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <Core/Defines.h>
#include <DataStreams/MarkInCompressedFile.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}


NativeBlockOutputStream::NativeBlockOutputStream(
    WriteBuffer & ostr_,
    UInt64 client_revision_,
    const Block & header_,
    WriteBuffer * index_ostr_,
    size_t initial_size_of_file_)
    : ostr(ostr_)
    , client_revision(client_revision_)
    , header(header_)
    , index_ostr(index_ostr_)
    , initial_size_of_file(initial_size_of_file_)
{
    if (index_ostr)
    {
        ostr_concrete = typeid_cast<CompressedWriteBuffer<> *>(&ostr);
        if (!ostr_concrete)
            throw Exception(
                "When need to write index for NativeBlockOutputStream, ostr must be CompressedWriteBuffer.",
                ErrorCodes::LOGICAL_ERROR);
    }
}


void NativeBlockOutputStream::flush()
{
    ostr.next();
}


void NativeBlockOutputStream::writeData(
    const IDataType & type,
    const ColumnPtr & column,
    WriteBuffer & ostr,
    size_t offset,
    size_t limit)
{
    /** If there are columns-constants - then we materialize them.
      * (Since the data type does not know how to serialize / deserialize constants.)
      */
    ColumnPtr full_column;

    if (ColumnPtr converted = column->convertToFullColumnIfConst())
        full_column = converted;
    else
        full_column = column;

    IDataType::OutputStreamGetter output_stream_getter = [&](const IDataType::SubstreamPath &) {
        return &ostr;
    };
    type.serializeBinaryBulkWithMultipleStreams(*full_column, output_stream_getter, offset, limit, false, {});
}


void NativeBlockOutputStream::write(const Block & block)
{
    /// Additional information about the block.
    if (client_revision > 0)
        block.info.write(ostr);

    block.checkNumberOfRows();

    /// Dimensions
    size_t columns = block.columns();
    size_t rows = block.rows();

    writeVarUInt(columns, ostr);
    writeVarUInt(rows, ostr);

    /** The index has the same structure as the data stream.
      * But instead of column values, it contains a mark that points to the location in the data file where this part of the column is located.
      */
    if (index_ostr)
    {
        writeVarUInt(columns, *index_ostr);
        writeVarUInt(rows, *index_ostr);
    }

    for (size_t i = 0; i < columns; ++i)
    {
        /// For the index.
        MarkInCompressedFile mark;

        if (index_ostr)
        {
            ostr_concrete->next(); /// Finish compressed block.
            mark.offset_in_compressed_file = initial_size_of_file + ostr_concrete->getCompressedBytes();
            mark.offset_in_decompressed_block = ostr_concrete->getRemainingBytes();
        }

        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);

        /// Name
        writeStringBinary(column.name, ostr);

        /// Type
        String type_name = column.type->getName();
        writeStringBinary(type_name, ostr);

        /// Data
        if (rows) /// Zero items of data is always represented as zero number of bytes.
            writeData(*column.type, column.column, ostr, 0, 0);

        if (index_ostr)
        {
            writeStringBinary(column.name, *index_ostr);
            writeStringBinary(column.type->getName(), *index_ostr);

            writeBinary(mark.offset_in_compressed_file, *index_ostr);
            writeBinary(mark.offset_in_decompressed_block, *index_ostr);
        }
    }
}

} // namespace DB
