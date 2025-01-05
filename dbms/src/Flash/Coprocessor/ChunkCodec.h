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

#pragma once

#include <Core/Block.h>
#include <Flash/Mpp/MppVersion.h>
#include <TiDB/Schema/TiDB.h>
#include <tipb/select.pb.h>
namespace DB
{
using DAGColumnInfo = std::pair<String, TiDB::ColumnInfo>;
using DAGSchema = std::vector<DAGColumnInfo>;

class ChunkCodecStream
{
public:
    explicit ChunkCodecStream(const std::vector<tipb::FieldType> & field_types_)
        : field_types(field_types_)
    {}
    virtual String getString() = 0;
    virtual void clear() = 0;
    virtual void encode(const Block & block, size_t start, size_t end) = 0;
    virtual ~ChunkCodecStream() = default;

protected:
    const std::vector<tipb::FieldType> & field_types;
};

class ChunkCodec
{
public:
    ChunkCodec() = default;
    virtual Block decode(const String & str, const DAGSchema & schema) = 0;

    virtual std::unique_ptr<ChunkCodecStream> newCodecStream(
        const std::vector<tipb::FieldType> & result_field_types,
        MppVersion mpp_version)
        = 0;

    virtual ~ChunkCodec() = default;
};

} // namespace DB
