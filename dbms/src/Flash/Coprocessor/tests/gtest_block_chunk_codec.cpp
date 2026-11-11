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

#include <Common/formatReadable.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Compression/CompressionInfo.h>
#include <IO/Compression/CompressionMethod.h>
#include <IO/WriteHelpers.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <common/logger_useful.h>

#include <initializer_list>
#include <random>


namespace DB::tests
{

// Return a block with **rows**, containing a random elems size array(f32) and 5 Int64 column.
static Block prepareBlock(size_t rows)
{
    Block block;
    size_t col_idx = 0;
    block.insert(ColumnGenerator::instance().generate({
        //
        rows,
        "Array(Float32)",
        RANDOM,
        fmt::format("col{}", col_idx),
        128,
        DataDistribution::RANDOM,
        3,
    }));
    ++col_idx;

    for (; col_idx < 5; ++col_idx)
    {
        DataTypePtr int64_data_type = std::make_shared<DataTypeInt64>();
        block.insert(ColumnGenerator::instance().generate({rows, "Int64", RANDOM, fmt::format("col{}", col_idx)}));
    }
    return block;
}

// Return a block with **rows**, containing a fixed elems size array(f32) and 5 Int64 column.
static Block prepareBlockWithFixedVecF32(size_t rows)
{
    Block block;
    size_t col_idx = 0;
    block.insert(ColumnGenerator::instance().generate({
        //
        rows,
        "Array(Float32)",
        RANDOM,
        fmt::format("col{}", col_idx),
        128,
        DataDistribution::FIXED,
        3,
    }));
    ++col_idx;

    for (; col_idx < 5; ++col_idx)
    {
        DataTypePtr int64_data_type = std::make_shared<DataTypeInt64>();
        block.insert(ColumnGenerator::instance().generate({rows, "Int64", RANDOM, fmt::format("col{}", col_idx)}));
    }
    return block;
}

template <typename VecCol>
void test_enocde_release_data(VecCol && batch_columns, const Block & header, const size_t total_rows)
{
    // encode and release columns
    const auto mode = CompressionMethod::LZ4;

    auto codec = CHBlockChunkCodecV1{
        header,
    };
    auto str = codec.encode(std::forward<VecCol>(batch_columns), mode);
    ASSERT_FALSE(str.empty());
    ASSERT_EQ(codec.encoded_rows, total_rows);
    ASSERT_NE(codec.compressed_size, 0);
    ASSERT_NE(codec.original_size, 0);
    auto decoded_block = CHBlockChunkCodecV1::decode(header, str);
    ASSERT_EQ(total_rows, decoded_block.rows());
    for (auto && columns : batch_columns)
    {
        for (auto && col : columns)
        {
            if (col)
                ASSERT_EQ(col->size(), 0);
        }
    }
    {
        // test no rows
        auto & empty_batch_columns = batch_columns;
        auto str = codec.encode(empty_batch_columns, mode);
        ASSERT_TRUE(str.empty());
    }
}

TEST(CHBlockChunkCodecTest, ChunkCodecV1)
try
{
    size_t block_num = 10;
    size_t rows = 10;
    std::vector<Block> blocks;
    auto header = prepareBlock(0);
    for (size_t i = 0; i < block_num; ++i)
    {
        auto block = prepareBlock(rows);
        blocks.emplace_back(std::move(block));
    }
    blocks.emplace_back(prepareBlock(0));

    auto total_rows = rows * block_num;
    for (auto mode : {CompressionMethod::NONE, CompressionMethod::LZ4, CompressionMethod::ZSTD})
    {
        {
            // encode nothing if no rows
            auto codec = CHBlockChunkCodecV1{
                header,
            };
            auto str = codec.encode(header, mode);
            ASSERT_TRUE(str.empty());
            ASSERT_EQ(codec.encoded_rows, 0);
            ASSERT_EQ(codec.compressed_size, 0);
            ASSERT_EQ(codec.original_size, 0);
        }
        {
            // test encode one block
            auto codec = CHBlockChunkCodecV1{
                header,
            };
            auto str = codec.encode(blocks.front(), mode);
            ASSERT_FALSE(str.empty());
            ASSERT_EQ(codec.encoded_rows, blocks.front().rows());
            auto decoded_block = CHBlockChunkCodecV1::decode(header, str);
            ASSERT_EQ(blocks.front().rows(), decoded_block.rows());
        }
        {
            // test encode blocks
            auto codec = CHBlockChunkCodecV1{
                header,
            };
            auto str = codec.encode(blocks, mode);
            ASSERT_FALSE(str.empty());
            ASSERT_EQ(codec.encoded_rows, total_rows);

            if (mode == CompressionMethod::NONE)
                ASSERT_EQ(codec.compressed_size, 0);
            else
                ASSERT_NE(codec.compressed_size, 0);

            ASSERT_NE(codec.original_size, 0);

            auto decoded_block = CHBlockChunkCodecV1::decode(header, str);
            ASSERT_EQ(total_rows, decoded_block.rows());
        }
        {
            std::vector<Block> blocks_to_move;
            blocks_to_move.reserve(blocks.size());
            for (auto && block : blocks)
            {
                blocks_to_move.emplace_back(block);
            }
            for (auto && block : blocks_to_move)
            {
                for (auto && col : block)
                {
                    ASSERT_TRUE(col.column);
                }
            }
            // test encode moved blocks
            auto codec = CHBlockChunkCodecV1{
                header,
            };
            auto str = codec.encode(std::move(blocks_to_move), mode);
            for (auto && block : blocks_to_move)
            {
                ASSERT_EQ(block.rows(), 0);
            }
            ASSERT_FALSE(str.empty());
            ASSERT_EQ(codec.encoded_rows, total_rows);

            if (mode == CompressionMethod::NONE)
                ASSERT_EQ(codec.compressed_size, 0);
            else
                ASSERT_NE(codec.compressed_size, 0);

            ASSERT_NE(codec.original_size, 0);

            auto decoded_block = CHBlockChunkCodecV1::decode(header, str);
            ASSERT_EQ(total_rows, decoded_block.rows());
        }
        {
            auto columns = prepareBlock(rows).getColumns();
            auto codec = CHBlockChunkCodecV1{
                header,
            };
            auto str = codec.encode(columns, mode);
            ASSERT_FALSE(str.empty());
            ASSERT_EQ(codec.encoded_rows, rows);
            auto decoded_block = CHBlockChunkCodecV1::decode(header, str);
            ASSERT_EQ(decoded_block.rows(), rows);
        }
        {
            auto columns = prepareBlock(rows).mutateColumns();
            auto codec = CHBlockChunkCodecV1{
                header,
            };
            auto str = codec.encode(columns, mode);
            ASSERT_FALSE(str.empty());
            ASSERT_EQ(codec.encoded_rows, rows);
            auto decoded_block = CHBlockChunkCodecV1::decode(header, str);
            ASSERT_EQ(decoded_block.rows(), rows);
        }
    }
    {
        std::vector<MutableColumns> batch_columns;
        for (size_t i = 0; i < block_num; ++i)
            batch_columns.emplace_back(prepareBlock(rows).mutateColumns());
        batch_columns.emplace_back(prepareBlock(0).mutateColumns());
        {
            auto tmp = prepareBlock(0).mutateColumns();
            for (auto && col : tmp)
            {
                col.reset();
            }
            batch_columns.emplace_back(std::move(tmp));
        }
        test_enocde_release_data(std::move(batch_columns), header, total_rows);
    }
    {
        std::vector<Columns> batch_columns;
        for (size_t i = 0; i < block_num; ++i)
            batch_columns.emplace_back(prepareBlock(rows).getColumns());
        batch_columns.emplace_back(prepareBlock(0).getColumns());
        {
            auto tmp = prepareBlock(0).getColumns();
            for (auto && col : tmp)
            {
                col.reset();
            }
            batch_columns.emplace_back(std::move(tmp));
        }
        test_enocde_release_data(std::move(batch_columns), header, total_rows);
    }
    {
        auto source_str = CHBlockChunkCodecV1{header}.encode(blocks.front(), CompressionMethod::NONE);
        ASSERT_FALSE(source_str.empty());
        ASSERT_EQ(static_cast<CompressionMethodByte>(source_str[0]), CompressionMethodByte::NONE);

        for (const auto method : {CompressionMethod::LZ4, CompressionMethod::ZSTD})
        {
            auto compressed_str_a = CHBlockChunkCodecV1::encode({&source_str[1], source_str.size() - 1}, method);
            auto compressed_str_b = CHBlockChunkCodecV1{header}.encode(blocks.front(), method);

            ASSERT_EQ(compressed_str_a, compressed_str_b);
        }
    }
}
CATCH

TEST(CHBlockChunkCodecTest, ChunkDecodeAndSquash)
try
{
    auto header = prepareBlockWithFixedVecF32(0);
    Blocks blocks = {
        prepareBlockWithFixedVecF32(11),
        prepareBlockWithFixedVecF32(17),
        prepareBlockWithFixedVecF32(23),
    };
    size_t num_rows = 0;

    CHBlockChunkCodecV1 codec(header);
    CHBlockChunkDecodeAndSquash decoder(header, 13);
    size_t num_rows_decoded = 0;
    Blocks blocks_decoded;
    auto check = [&](std::optional<Block> && block_opt) {
        if (block_opt)
        {
            block_opt->checkNumberOfRows();
            num_rows_decoded += block_opt->rows();
            blocks_decoded.emplace_back(std::move(*block_opt));
        }
    };
    for (const auto & b : blocks)
    {
        num_rows += b.rows();
        LOG_DEBUG(Logger::get(), "ser/deser block {}", getColumnsContent(b.getColumnsWithTypeAndName()));
        auto str = codec.encode(b, CompressionMethod::LZ4);
        check(decoder.decodeAndSquashV1(str));
    }
    check(decoder.flush());
    ASSERT_EQ(num_rows, num_rows_decoded);

    auto input_block = vstackBlocks(std::move(blocks));
    auto decoded_block = vstackBlocks(std::move(blocks_decoded));
    ASSERT_BLOCK_EQ(input_block, decoded_block);
}
CATCH


TEST(CHBlockChunkCodecTest, ChunkDecodeAndSquashRandom)
try
{
    std::mt19937_64 rand_gen;

    auto header = prepareBlockWithFixedVecF32(0);
    size_t num_blocks = std::uniform_int_distribution<Int32>(1, 64)(rand_gen);
    size_t num_rows = 0;
    Blocks blocks;
    for (size_t i = 0; i < num_blocks; ++i)
    {
        auto b = prepareBlockWithFixedVecF32(std::uniform_int_distribution<>(0, 8192)(rand_gen));
        num_rows += b.rows();
        blocks.emplace_back(std::move(b));
    }

    LOG_DEBUG(Logger::get(), "generate blocks, num_blocks={} num_rows={}", num_blocks, num_rows);

    CHBlockChunkCodecV1 codec(header);
    CHBlockChunkDecodeAndSquash decoder(header, 1024);
    size_t num_rows_decoded = 0;
    size_t num_bytes = 0;
    Blocks blocks_decoded;
    auto check = [&](std::optional<Block> && block_opt) {
        if (block_opt)
        {
            block_opt->checkNumberOfRows();
            num_rows_decoded += block_opt->rows();
            blocks_decoded.emplace_back(std::move(*block_opt));
        }
    };
    for (const auto & b : blocks)
    {
        // LOG_DEBUG(Logger::get(), "ser/deser block {}", getColumnsContent(b.getColumnsWithTypeAndName()));
        auto str = codec.encode(b, CompressionMethod::LZ4);
        num_bytes += str.size();
        check(decoder.decodeAndSquashV1(str));
    }
    check(decoder.flush());
    ASSERT_EQ(num_rows, num_rows_decoded);
    LOG_DEBUG(
        Logger::get(),
        "ser/deser done, num_blocks={} num_rows={} num_bytes={}",
        num_blocks,
        num_rows,
        formatReadableSizeWithBinarySuffix(num_bytes));

    auto input_block = vstackBlocks(std::move(blocks));
    auto decoded_block = vstackBlocks(std::move(blocks_decoded));
    ASSERT_BLOCK_EQ(input_block, decoded_block);
}
CATCH

namespace
{
void writeRowCountHeader(WriteBuffer & output, const Block & header, size_t rows)
{
    writeVarUInt(header.columns(), output);
    writeVarUInt(rows, output);
    for (const auto & column : header)
    {
        writeStringBinary(column.name, output);
        writeStringBinary(column.type->getName(), output);
    }
}

String encodeRowCountChunk(
    const Block & header,
    size_t rows,
    MPPDataPacketVersion version,
    CompressionMethod compression,
    std::initializer_list<size_t> sub_block_rows = {})
{
    WriteBufferFromOwnString output;
    writeRowCountHeader(output, header, rows);
    if (version == MPPDataPacketV0)
    {
        if (rows == 1)
            writeBinary(UInt64{42}, output);
        return output.releaseStr();
    }
    for (const auto sub_rows : sub_block_rows)
    {
        writeVarUInt(sub_rows, output);
        // Only one-row sub-blocks carry data; other sizes exercise validation before reading data.
        if (sub_rows == 1)
            writeBinary(UInt64{42}, output);
    }
    auto raw = output.releaseStr();
    if (compression == CompressionMethod::NONE)
        return String(1, static_cast<char>(CompressionMethodByte::NONE)) + raw;
    return CHBlockChunkCodecV1::encode(raw, compression);
}

template <typename Decode>
void expectRowCountError(Decode && decode)
{
    try
    {
        decode();
        FAIL() << "Expected invalid row counts to be rejected";
    }
    catch (const Exception & e)
    {
        EXPECT_NE(e.message().find("row count"), String::npos) << e.displayText();
    }
}
} // namespace

TEST(CHBlockChunkCodecTest, RejectOversizedRowCounts)
try
{
    const Block header({createColumn<UInt64>({}, "value")});
    for (const auto version : {MPPDataPacketV0, MPPDataPacketV1})
    {
        for (const auto compression : {CompressionMethod::NONE, CompressionMethod::LZ4, CompressionMethod::ZSTD})
        {
            if (version == MPPDataPacketV0 && compression != CompressionMethod::NONE)
                continue;
            for (const auto rows : {CodecUtils::MAX_DECODED_BLOCK_ROWS + 1, size_t{1} << 60})
            {
                SCOPED_TRACE(fmt::format(
                    "version={} compression={} rows={}",
                    static_cast<int>(version),
                    static_cast<int>(compression),
                    rows));
                const auto chunk = encodeRowCountChunk(header, rows, version, compression);
                expectRowCountError([&] {
                    return version == MPPDataPacketV0 ? CHBlockChunkCodec::decode(chunk, header)
                                                      : CHBlockChunkCodecV1::decode(header, chunk);
                });
                CHBlockChunkDecodeAndSquash decoder(header, 8192);
                expectRowCountError([&] {
                    return version == MPPDataPacketV0 ? decoder.decodeAndSquash(chunk)
                                                      : decoder.decodeAndSquashV1(chunk);
                });
            }

            // Each chunk is within the limit, but appending the second would exceed it.
            CHBlockChunkDecodeAndSquash decoder(header, 8192);
            const auto decode = [&](const String & chunk) {
                return version == MPPDataPacketV0 ? decoder.decodeAndSquash(chunk) : decoder.decodeAndSquashV1(chunk);
            };
            ASSERT_FALSE(decode(encodeRowCountChunk(header, 1, version, compression, {1})));
            const auto chunk = encodeRowCountChunk(header, CodecUtils::MAX_DECODED_BLOCK_ROWS, version, compression);
            expectRowCountError([&] { return decode(chunk); });
        }
    }
}
CATCH

TEST(CHBlockChunkCodecTest, RejectInvalidSubBlockRowCounts)
try
{
    const Block header({createColumn<UInt64>({}, "value")});
    for (const auto compression : {CompressionMethod::NONE, CompressionMethod::LZ4, CompressionMethod::ZSTD})
    {
        for (const auto sub_rows : {size_t{0}, size_t{2}, size_t{1} << 60})
        {
            const auto chunk = encodeRowCountChunk(header, 1, MPPDataPacketV1, compression, {sub_rows});
            expectRowCountError([&] { return CHBlockChunkCodecV1::decode(header, chunk); });
        }
        // The second sub-block must fit the remaining rows, not just the chunk total.
        const auto chunk = encodeRowCountChunk(header, 2, MPPDataPacketV1, compression, {1, 2});
        expectRowCountError([&] { return CHBlockChunkCodecV1::decode(header, chunk); });
        const auto valid = encodeRowCountChunk(header, 2, MPPDataPacketV1, compression, {1, 1});
        const auto decoded = CHBlockChunkCodecV1::decode(header, valid);
        ASSERT_EQ(decoded.rows(), 2);
        EXPECT_EQ(decoded.getByPosition(0).column->getUInt(0), 42);
        EXPECT_EQ(decoded.getByPosition(0).column->getUInt(1), 42);
    }
}
CATCH

TEST(CHBlockChunkCodecTest, AcceptRowCountHeaderBoundaries)
try
{
    const Block header({createColumn<UInt64>({}, "value")});
    for (const auto rows : {size_t{0}, CodecUtils::MAX_DECODED_BLOCK_ROWS})
    {
        WriteBufferFromOwnString output;
        writeRowCountHeader(output, header, rows);
        ReadBufferFromString input(output.str());
        size_t decoded_rows = 0;
        const auto decoded_header = DecodeHeader(input, header, decoded_rows);
        EXPECT_EQ(decoded_rows, rows);
        EXPECT_EQ(decoded_header.columns(), header.columns());
    }
}
CATCH
} // namespace DB::tests
