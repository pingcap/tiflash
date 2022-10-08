// Copyright 2022 PingCAP, Ltd.
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

#include <Debug/dbgFuncCoprocessorUtils.h>

namespace DB
{
std::unique_ptr<ChunkCodec> getCodec(tipb::EncodeType encode_type)
{
    switch (encode_type)
    {
    case tipb::EncodeType::TypeDefault:
        return std::make_unique<DefaultChunkCodec>();
    case tipb::EncodeType::TypeChunk:
        return std::make_unique<ArrowChunkCodec>();
    case tipb::EncodeType::TypeCHBlock:
        return std::make_unique<CHBlockChunkCodec>();
    default:
        throw Exception("Unsupported encode type", ErrorCodes::BAD_ARGUMENTS);
    }
}

DAGSchema getSelectSchema(Context & context)
{
    DAGSchema schema;
    auto * dag_context = context.getDAGContext();
    auto result_field_types = dag_context->result_field_types;
    for (size_t i = 0; i < result_field_types.size(); ++i)
    {
        ColumnInfo info = TiDB::fieldTypeToColumnInfo(result_field_types[i]);
        String col_name = "col_" + std::to_string(i);
        schema.push_back(std::make_pair(col_name, info));
    }
    return schema;
}

SortDescription generateSDFromSchema(const DAGSchema & schema)
{
    SortDescription sort_desc;
    sort_desc.reserve(schema.size());
    for (const auto & col : schema)
    {
        sort_desc.emplace_back(col.first, -1, -1, nullptr);
    }
    return sort_desc;
}

void chunksToBlocks(const DAGSchema & schema, const tipb::SelectResponse & dag_response, BlocksList & blocks)
{
    auto codec = getCodec(dag_response.encode_type());
    for (const auto & chunk : dag_response.chunks())
        blocks.emplace_back(codec->decode(chunk.rows_data(), schema));
}

BlockInputStreamPtr outputDAGResponse(Context &, const DAGSchema & schema, const tipb::SelectResponse & dag_response)
{
    if (dag_response.has_error())
        throw Exception(dag_response.error().msg(), dag_response.error().code());

    BlocksList blocks;
    chunksToBlocks(schema, dag_response, blocks);
    return std::make_shared<BlocksListBlockInputStream>(std::move(blocks));
}

// Just for test usage, dag_response should not contain result more than 128M
Block getMergedBigBlockFromDagRsp(Context & context, const DAGSchema & schema, const tipb::SelectResponse & dag_response)
{
    auto src = outputDAGResponse(context, schema, dag_response);
    // Try to merge into big block. 128 MB should be enough.
    SquashingBlockInputStream squashed_delete_stream(src, 0, 128 * (1UL << 20), /*req_id=*/"");
    Blocks result_data;
    while (true)
    {
        Block block = squashed_delete_stream.read();
        if (!block)
        {
            if (result_data.empty())
            {
                // Ensure that result_data won't be empty in any situation
                result_data.emplace_back(std::move(block));
            }
            break;
        }
        else
        {
            result_data.emplace_back(std::move(block));
        }
    }

    if (result_data.size() > 1)
        throw Exception("Result block should be less than 128M!", ErrorCodes::BAD_ARGUMENTS);
    return result_data[0];
}

bool dagRspEqual(Context & context, const tipb::SelectResponse & expected, const tipb::SelectResponse & actual, String & unequal_msg)
{
    auto schema = getSelectSchema(context);
    SortDescription sort_desc = generateSDFromSchema(schema);
    Block block_a = getMergedBigBlockFromDagRsp(context, schema, expected);
    sortBlock(block_a, sort_desc);
    Block block_b = getMergedBigBlockFromDagRsp(context, schema, actual);
    sortBlock(block_b, sort_desc);
    bool equal = blockEqual(block_a, block_b, unequal_msg);
    if (!equal)
    {
        unequal_msg = fmt::format("{}\nExpected Results: \n{}\nActual Results: \n{}", unequal_msg, formatBlockData(block_a), formatBlockData(block_b));
    }
    return equal;
}

static const String ENCODE_TYPE_NAME = "encode_type";
static const String TZ_OFFSET_NAME = "tz_offset";
static const String TZ_NAME_NAME = "tz_name";
static const String COLLATOR_NAME = "collator";
static const String MPP_QUERY = "mpp_query";
static const String USE_BROADCAST_JOIN = "use_broadcast_join";
static const String MPP_PARTITION_NUM = "mpp_partition_num";
static const String MPP_TIMEOUT = "mpp_timeout";

DAGProperties getDAGProperties(const String & prop_string)
{
    DAGProperties ret;
    if (prop_string.empty())
        return ret;
    std::unordered_map<String, String> properties;
    Poco::StringTokenizer string_tokens(prop_string, ",");
    for (const auto & string_token : string_tokens)
    {
        Poco::StringTokenizer tokens(string_token, ":");
        if (tokens.count() != 2)
            continue;
        properties[Poco::toLower(tokens[0])] = tokens[1];
    }

    if (properties.find(ENCODE_TYPE_NAME) != properties.end())
        ret.encode_type = properties[ENCODE_TYPE_NAME];
    if (properties.find(TZ_OFFSET_NAME) != properties.end())
        ret.tz_offset = std::stol(properties[TZ_OFFSET_NAME]);
    if (properties.find(TZ_NAME_NAME) != properties.end())
        ret.tz_name = properties[TZ_NAME_NAME];
    if (properties.find(COLLATOR_NAME) != properties.end())
        ret.collator = std::stoi(properties[COLLATOR_NAME]);
    if (properties.find(MPP_QUERY) != properties.end())
        ret.is_mpp_query = properties[MPP_QUERY] == "true";
    if (properties.find(USE_BROADCAST_JOIN) != properties.end())
        ret.use_broadcast_join = properties[USE_BROADCAST_JOIN] == "true";
    if (properties.find(MPP_PARTITION_NUM) != properties.end())
        ret.mpp_partition_num = std::stoi(properties[MPP_PARTITION_NUM]);
    if (properties.find(MPP_TIMEOUT) != properties.end())
        ret.mpp_timeout = std::stoi(properties[MPP_TIMEOUT]);

    return ret;
}

} // namespace DB
