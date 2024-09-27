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

#include <DataTypes/DataTypesNumber.h>
#include <Flash/Coprocessor/CHBlockChunkCodecV1.h>
#include <Flash/Coprocessor/ChunkDecodeAndSquash.h>
#include <IO/Buffer/ReadBufferFromString.h>
#include <IO/Compression/CompressionMethod.h>
#include <TestUtils/ColumnGenerator.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>


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
{
    auto header = prepareBlockWithFixedVecF32(0);
    Blocks blocks = {
        prepareBlockWithFixedVecF32(11),
        prepareBlockWithFixedVecF32(17),
        prepareBlockWithFixedVecF32(23),
    };

    CHBlockChunkCodecV1 codec(header);
    CHBlockChunkDecodeAndSquash decoder(header, 13);
    for (const auto & b : blocks)
    {
        LOG_INFO(Logger::get(), "ser/deser block {}", getColumnsContent(b.getColumnsWithTypeAndName()));
        auto str = codec.encode(b, CompressionMethod::LZ4);
        decoder.decodeAndSquashV1(str);
    }
}

} // namespace DB::tests
