// Copyright 2025 PingCAP, Inc.
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

#include <Common/config.h>

#if ENABLE_CLARA
#include <Core/ColumnWithTypeAndName.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/Index/FullTextIndex/Stream/InputStream.h>
#include <Storages/DeltaMerge/Index/LocalIndexCache.h>
#include <Storages/DeltaMerge/Index/LocalIndexInfo.h>
#include <Storages/DeltaMerge/tests/gtest_dm_delta_merge_store_test_basic.h>
#include <Storages/DeltaMerge/tests/gtest_segment_util.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/InputStreamTestUtils.h>
#include <TiDB/Decode/DatumCodec.h>
#include <TiDB/Schema/TiDB.h>


namespace DB::DM::tests
{

class FullTextIndexTestUtils
{
public:
    static constexpr ColumnID fts_column_id = 130;
    static constexpr const char * fts_column_name = "body";
    static constexpr IndexID fts_index_id = 42;

    static ColumnDefine cdFts()
    {
        return ColumnDefine(fts_column_id, fts_column_name, ::DB::tests::typeFromString("StringV2"));
    }

    static ColumnWithTypeAndName colInt64(std::string_view sequence, const String & name = "", Int64 column_id = 0)
    {
        auto data = genSequence<Int64>(sequence);
        return ::DB::tests::createColumn<Int64>(data, name, column_id);
    }

    /// Create a column with values like "word_1", "word_2", "word_3", ...
    static ColumnWithTypeAndName colString(std::string_view sequence, const String & name = "", Int64 column_id = 0)
    {
        auto data = genSequence<Int64>(sequence);
        std::vector<String> column_data;
        column_data.reserve(data.size());
        for (auto & v : data)
            column_data.push_back(fmt::format("word_{}", v));
        return ::DB::tests::createColumn<String>(column_data, name, column_id);
    }

    struct FtsQueryInfoTopKOptions
    {
        String query;
        UInt32 top_k;
        Int64 column_id = 130; // fts_column_id
        Int64 index_id = 42; // fts_index_id
    };

    static FTSQueryInfoPtr ftsQueryInfoTopK(FtsQueryInfoTopKOptions options)
    {
        auto fts_query_info = std::make_shared<tipb::FTSQueryInfo>();
        fts_query_info->set_query_type(tipb::FTSQueryType::FTSQueryTypeWithScore);
        fts_query_info->set_index_id(options.index_id);
        auto * column_info = fts_query_info->add_columns();
        column_info->set_column_id(options.column_id);
        column_info->set_tp(TiDB::TP::TypeString);
        column_info->set_flag(TiDB::ColumnFlagNotNull);
        fts_query_info->set_top_k(options.top_k);
        fts_query_info->set_query_text(options.query);
        fts_query_info->set_query_tokenizer("STANDARD_V1");
        return fts_query_info;
    }

    static LocalIndexInfosPtr indexInfo(
        TiDB::FullTextIndexDefinition definition = TiDB::FullTextIndexDefinition{
            .parser_type = "STANDARD_V1",
        })
    {
        const LocalIndexInfos index_infos = LocalIndexInfos{
            LocalIndexInfo(fts_index_id, fts_column_id, std::make_shared<TiDB::FullTextIndexDefinition>(definition)),
        };
        return std::make_shared<LocalIndexInfos>(index_infos);
    }

    static auto wrapFTSStream(
        const FullTextIndexStreamCtxPtr & ctx,
        const SkippableBlockInputStreamPtr & inner,
        const BitmapFilterPtr & filter)
    {
        auto stream = ConcatSkippableBlockInputStream<false>::create(
            /* inputs */ {inner},
            /* rows */ {filter->size()},
            /* ScanContext */ nullptr);
        return FullTextIndexInputStream::create(ctx, filter, stream);
    }
};

} // namespace DB::DM::tests
#endif
