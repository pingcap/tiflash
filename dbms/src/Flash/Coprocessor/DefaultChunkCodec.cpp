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

#include <Flash/Coprocessor/DefaultChunkCodec.h>
#include <TiDB/Decode/Datum.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Decode/TypeMapping.h>
#include <TiDB/Schema/TiDB.h>

using TiDB::DatumBumpy;
using TiDB::DatumFlat;
using TiDB::TP;

namespace DB
{
class DefaultChunkCodecStream : public ChunkCodecStream
{
public:
    explicit DefaultChunkCodecStream(const std::vector<tipb::FieldType> & field_types)
        : ChunkCodecStream(field_types)
    {}
    WriteBufferFromOwnString ss;
    String getString() override { return ss.releaseStr(); }
    void encode(const Block & block, size_t start, size_t end) override;
    void clear() override { ss.restart(); }
};

void DefaultChunkCodecStream::encode(const Block & block, size_t start, size_t end)
{
    // TODO: Check compatibility between field_tp_and_flags and block column types.
    // Encode data to chunk by default encode
    for (size_t i = start; i < end; i++)
    {
        for (size_t j = 0; j < block.columns(); j++)
        {
            const auto & field = (*block.getByPosition(j).column.get())[i];
            DatumBumpy datum(field, static_cast<TP>(field_types[j].tp()));
            EncodeDatum(datum.field(), getCodecFlagByFieldType(field_types[j]), ss);
        }
    }
}

Block DefaultChunkCodec::decode(const String & data, const DAGSchema & schema)
{
    std::vector<std::vector<Field>> rows;
    std::vector<Field> curr_row;
    size_t cursor = 0;
    while (cursor < data.size())
    {
        curr_row.push_back(DecodeDatumForCHRow(cursor, data, schema[curr_row.size()].second));
        if (curr_row.size() == schema.size())
        {
            rows.emplace_back(std::move(curr_row));
            curr_row.clear();
        }
    }

    ColumnsWithTypeAndName columns;
    for (const auto & field : schema)
    {
        const auto & name = field.first;
        auto data_type = getDataTypeByColumnInfoForComputingLayer(field.second);
        ColumnWithTypeAndName col(data_type, name);
        col.column->assumeMutable()->reserve(rows.size());
        columns.emplace_back(std::move(col));
    }
    for (const auto & row : rows)
    {
        for (size_t i = 0; i < row.size(); i++)
        {
            const Field & field = row[i];
            columns[i].column->assumeMutable()->insert(DatumFlat(field, schema[i].second.tp).field());
        }
    }
    return Block(columns);
}

std::unique_ptr<ChunkCodecStream> DefaultChunkCodec::newCodecStream(const std::vector<tipb::FieldType> & field_types, MppVersion)
{
    return std::make_unique<DefaultChunkCodecStream>(field_types);
}

} // namespace DB
