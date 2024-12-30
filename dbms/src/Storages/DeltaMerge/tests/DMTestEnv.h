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

#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <TestUtils/FunctionTestUtils.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <TiDB/Schema/TiDB.h>

#include <vector>

namespace DB
{
namespace DM
{
namespace tests
{
#define GET_REGION_RANGE(start, end, table_id) \
    RowKeyRange::fromHandleRange(::DB::DM::HandleRange((start), (end))).toRegionRange((table_id))

// Add this so that we can call typeFromString under namespace DB::DM::tests
using DB::tests::typeFromString;

using namespace DB::tests;

/// helper functions for comparing HandleRange
inline ::testing::AssertionResult HandleRangeCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const HandleRange & lhs,
    const HandleRange & rhs)
{
    if (lhs == rhs)
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.toDebugString(), rhs.toDebugString(), false);
}
/// helper functions for comparing HandleRange
inline ::testing::AssertionResult RowKeyRangeCompare(
    const char * lhs_expr,
    const char * rhs_expr,
    const RowKeyRange & lhs,
    const RowKeyRange & rhs)
{
    if (lhs.is_common_handle == rhs.is_common_handle && lhs == rhs)
        return ::testing::AssertionSuccess();
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.toDebugString(), rhs.toDebugString(), false);
}
#define ASSERT_RANGE_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::DM::tests::HandleRangeCompare, val1, val2)
#define ASSERT_ROWKEY_RANGE_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::DM::tests::RowKeyRangeCompare, val1, val2)
#define EXPECT_RANGE_EQ(val1, val2) EXPECT_PRED_FORMAT2(::DB::DM::tests::HandleRangeCompare, val1, val2)
#define GET_GTEST_FULL_NAME                                                                     \
    (String() + ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name() + "." \
     + ::testing::UnitTest::GetInstance()->current_test_info()->name())

inline Strings createNumberStrings(size_t beg, size_t end)
{
    Strings values;
    for (size_t i = beg; i < end; ++i)
        values.emplace_back(DB::toString(i));
    return values;
}

template <typename T>
inline std::vector<T> createNumbers(size_t beg, size_t end, bool reversed = false)
{
    std::vector<T> values;
    size_t num_rows = end - beg;
    for (size_t i = 0; i < num_rows; ++i)
        values.emplace_back(reversed ? static_cast<T>(end - 1 - i) : static_cast<T>(beg + i));
    return values;
}

inline std::vector<Int64> createSignedNumbers(size_t beg, size_t end)
{
    std::vector<Int64> values;
    for (size_t i = beg; i < end; ++i)
        values.emplace_back(i * (i % 2 == 0 ? -1 : 1));
    return values;
}

// Mock a common_pk_col that composed by number `rowkey_column_size` of int64 value
inline String genMockCommonHandle(Int64 value, size_t rowkey_column_size)
{
    WriteBufferFromOwnString ss;
    for (size_t index = 0; index < rowkey_column_size; ++index)
    {
        ::DB::EncodeUInt(static_cast<UInt8>(TiDB::CodecFlagInt), ss);
        ::DB::EncodeInt64(value, ss);
    }
    return ss.releaseStr();
}

class DMTestEnv
{
public:
    static ContextPtr getContext() { return ::DB::tests::TiFlashTestEnv::getContext(); }
    static ContextPtr getContext(const ::DB::Settings & settings)
    {
        return ::DB::tests::TiFlashTestEnv::getContext(settings);
    }

    static constexpr const char * pk_name = "_tidb_rowid";

    static constexpr const char * PK_NAME_PK_IS_HANDLE = "id";

    static constexpr ColId PK_ID_PK_IS_HANDLE = 2;

    enum class PkType
    {
        // If the primary key is composed of multiple columns and non-clustered-index,
        // or users don't define the primary key, TiDB will add a hidden "_tidb_rowid" column
        // as the handle column
        HiddenTiDBRowID,
        // Common handle for clustered-index since 5.0.0
        CommonHandle,
        // If user define the primary key that is compatibility with UInt64, use that column
        // as the handle column
        PkIsHandleInt64,
        PkIsHandleInt32,
    };

    static String PkTypeToString(PkType type)
    {
        switch (type)
        {
        case PkType::HiddenTiDBRowID:
            return "HiddenTiDBRowID";
        case PkType::CommonHandle:
            return "CommonHandle";
        case PkType::PkIsHandleInt64:
            return "PkIsHandleInt64";
        case PkType::PkIsHandleInt32:
            return "PkIsHandleInt32";
        }
        return "<unknown>";
    }

    static ColumnDefinesPtr getDefaultColumns(PkType pk_type = PkType::HiddenTiDBRowID, bool add_nullable = false)
    {
        // Return [handle, ver, del] column defines
        ColumnDefinesPtr columns = std::make_shared<ColumnDefines>();
        switch (pk_type)
        {
        case PkType::HiddenTiDBRowID:
            columns->emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/false));
            break;
        case PkType::CommonHandle:
            columns->emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/true));
            break;
        case PkType::PkIsHandleInt64:
            columns->emplace_back(
                ColumnDefine{PK_ID_PK_IS_HANDLE, PK_NAME_PK_IS_HANDLE, MutSup::getExtraHandleColumnIntType()});
            break;
        case PkType::PkIsHandleInt32:
            columns->emplace_back(
                ColumnDefine{PK_ID_PK_IS_HANDLE, PK_NAME_PK_IS_HANDLE, DataTypeFactory::instance().get("Int32")});
            break;
        default:
            throw Exception("Unknown pk type for test");
        }
        columns->emplace_back(getVersionColumnDefine());
        columns->emplace_back(getTagColumnDefine());
        if (add_nullable)
        {
            columns->emplace_back(
                ColumnDefine{1, "Nullable(UInt64)", DataTypeFactory::instance().get("Nullable(UInt64)")});
        }
        return columns;
    }

    /// Returns a NamesAndTypesList that can be used to construct StorageDeltaMerge.
    static NamesAndTypesList getDefaultTableColumns(PkType pk_type = PkType::HiddenTiDBRowID)
    {
        NamesAndTypesList columns;
        switch (pk_type)
        {
        case PkType::HiddenTiDBRowID:
            columns.push_back({MutSup::extra_handle_column_name, MutSup::getExtraHandleColumnIntType()});
            break;
        case PkType::CommonHandle:
            columns.push_back(
                {PK_NAME_PK_IS_HANDLE,
                 MutSup::
                     getExtraHandleColumnStringType()}); // For common handle, there must be a user-given primary key.
            columns.push_back(
                {MutSup::extra_handle_column_name,
                 MutSup::getExtraHandleColumnStringType()}); // For common handle, a _tidb_rowid is also constructed.
            break;
        case PkType::PkIsHandleInt64:
            columns.emplace_back(PK_NAME_PK_IS_HANDLE, MutSup::getExtraHandleColumnIntType());
            break;
        case PkType::PkIsHandleInt32:
            throw Exception("PkIsHandleInt32 is unsupported");
        default:
            throw Exception("Unknown pk type for test");
        }
        return columns;
    }

    /// Returns a TableInfo that can be used to construct StorageDeltaMerge.
    static TiDB::TableInfo getMinimalTableInfo(TableID table_id, PkType pk_type = PkType::HiddenTiDBRowID)
    {
        TiDB::TableInfo table_info;
        table_info.id = table_id;
        switch (pk_type)
        {
        case PkType::HiddenTiDBRowID:
            table_info.is_common_handle = false;
            table_info.pk_is_handle = false;
            break;
        case PkType::CommonHandle:
        {
            table_info.is_common_handle = true;
            table_info.pk_is_handle = false;
            TiDB::ColumnInfo pk_column; // For common handle, there must be a user-given primary key.
            pk_column.id = PK_ID_PK_IS_HANDLE;
            pk_column.name = PK_NAME_PK_IS_HANDLE;
            pk_column.setPriKeyFlag();
            table_info.columns.push_back(pk_column);
            break;
        }
        case PkType::PkIsHandleInt64:
        {
            table_info.is_common_handle = false;
            table_info.pk_is_handle = true;
            TiDB::ColumnInfo pk_column;
            pk_column.id = PK_ID_PK_IS_HANDLE;
            pk_column.name = PK_NAME_PK_IS_HANDLE;
            pk_column.setPriKeyFlag();
            table_info.columns.push_back(pk_column);
            break;
        }
        case PkType::PkIsHandleInt32:
            throw Exception("PkIsHandleInt32 is unsupported");
        default:
            throw Exception("Unknown pk type for test");
        }
        return table_info;
    }

    /// Return a ASTPtr that can be used to construct StorageDeltaMerge.
    static ASTPtr getPrimaryKeyExpr(const String & table_name, PkType pk_type = PkType::HiddenTiDBRowID)
    {
        ASTPtr astptr(new ASTIdentifier(table_name, ASTIdentifier::Kind::Table));
        String name;
        switch (pk_type)
        {
        case PkType::HiddenTiDBRowID:
            name = MutSup::extra_handle_column_name;
            break;
        case PkType::CommonHandle:
            name = MutSup::extra_handle_column_name;
            break;
        case PkType::PkIsHandleInt64:
            name = PK_NAME_PK_IS_HANDLE;
            break;
        case PkType::PkIsHandleInt32:
            throw Exception("PkIsHandleInt32 is unsupported");
        default:
            throw Exception("Unknown pk type for test");
        }
        astptr->children.emplace_back(new ASTIdentifier(name));
        return astptr;
    }

    /**
     * Create a simple block with 3 columns:
     *   * `pk` - Int64 / `version` / `tag`
     * @param beg       `pk`'s value begin
     * @param end       `pk`'s value end (not included)
     * @param reversed  increasing/decreasing insert `pk`'s value
     * @return
     */
    static Block prepareSimpleWriteBlock(
        size_t beg,
        size_t end,
        bool reversed,
        UInt64 tso = 2,
        const String & pk_name_ = pk_name,
        ColumnID pk_col_id = MutSup::extra_handle_id,
        DataTypePtr pk_type = MutSup::getExtraHandleColumnIntType(),
        bool is_common_handle = false,
        size_t rowkey_column_size = 1,
        bool with_internal_columns = true,
        bool is_deleted = false,
        bool with_nullable_uint64 = false)
    {
        Block block;
        const size_t num_rows = (end - beg);
        if (is_common_handle)
        {
            // common_pk_col
            Strings values;
            for (size_t i = 0; i < num_rows; i++)
            {
                Int64 value = reversed ? end - 1 - i : beg + i;
                values.emplace_back(genMockCommonHandle(value, rowkey_column_size));
            }
            block.insert(DB::tests::createColumn<String>(std::move(values), pk_name_, pk_col_id));
        }
        else
        {
            // int-like pk_col
            block.insert(ColumnWithTypeAndName{
                DB::tests::makeColumn<Int64>(pk_type, createNumbers<Int64>(beg, end, reversed)),
                pk_type,
                pk_name_,
                pk_col_id});
            // add extra column if need
            if (pk_col_id != MutSup::extra_handle_id)
            {
                block.insert(ColumnWithTypeAndName{
                    DB::tests::makeColumn<Int64>(
                        MutSup::getExtraHandleColumnIntType(),
                        createNumbers<Int64>(beg, end, reversed)),
                    MutSup::getExtraHandleColumnIntType(),
                    MutSup::extra_handle_column_name,
                    MutSup::extra_handle_id});
            }
        }
        if (with_internal_columns)
        {
            // version_col
            block.insert(DB::tests::createColumn<UInt64>(
                std::vector<UInt64>(num_rows, tso),
                MutSup::version_column_name,
                MutSup::version_col_id));
            // tag_col
            block.insert(DB::tests::createColumn<UInt8>(
                std::vector<UInt64>(num_rows, is_deleted),
                MutSup::delmark_column_name,
                MutSup::delmark_col_id));
        }
        if (with_nullable_uint64)
        {
            std::vector<UInt64> data(num_rows);
            std::iota(data.begin(), data.end(), beg);
            std::vector<Int32> null_map(num_rows, 0);
            block.insert(DB::tests::createNullableColumn<UInt64>(data, null_map, "Nullable(UInt64)", 1));
        }
        return block;
    }

    static Block prepareSimpleWriteBlockWithNullable(size_t beg, size_t end)
    {
        return prepareSimpleWriteBlock(
            beg,
            end,
            /*reversed*/ false,
            /*tso*/ 2,
            pk_name,
            MutSup::extra_handle_id,
            MutSup::getExtraHandleColumnIntType(),
            /* is_common_handle */ false,
            /* rowkey_column_size */ 1,
            /*with_internal_columns*/ true,
            /*is_deleted*/ false,
            /*with_nullable_uint64*/ true);
    }
    /**
     * Create a simple block with 3 columns:
     *   * `pk` - Int64 / `version` / `tag`
     * @param beg       `pk`'s value begin
     * @param end       `pk`'s value end (not included)
     * @param reversed  increasing/decreasing insert `pk`'s value
     * @return
     */
    static Block prepareSimpleWriteBlock(
        size_t beg,
        size_t end,
        bool reversed,
        PkType pk_type,
        UInt64 tso = 2,
        bool with_internal_columns = true)
    {
        switch (pk_type)
        {
        case PkType::HiddenTiDBRowID:
            return prepareSimpleWriteBlock(
                beg,
                end,
                reversed,
                tso,
                MutSup::extra_handle_column_name,
                MutSup::extra_handle_id,
                MutSup::getExtraHandleColumnIntType(),
                false,
                1,
                with_internal_columns);
        case PkType::CommonHandle:
            return prepareSimpleWriteBlock(
                beg,
                end,
                reversed,
                tso,
                MutSup::extra_handle_column_name,
                MutSup::extra_handle_id,
                MutSup::getExtraHandleColumnStringType(),
                true,
                1,
                with_internal_columns);
        case PkType::PkIsHandleInt64:
            return prepareSimpleWriteBlock(
                beg,
                end,
                reversed,
                tso,
                PK_NAME_PK_IS_HANDLE,
                PK_ID_PK_IS_HANDLE,
                MutSup::getExtraHandleColumnIntType(),
                false,
                1,
                with_internal_columns);
            break;
        case PkType::PkIsHandleInt32:
            throw Exception("PkIsHandleInt32 is unsupported");
        default:
            throw Exception("Unknown pk type for test");
        }
    }

    /**
     * Create a simple block with 3 columns:
     *   * `pk` - Int64 / `version` / `tag`
     * @param pk        `pk`'s value
     * @param ts_beg    `timestamp`'s value begin
     * @param ts_end    `timestamp`'s value end (not included)
     * @param reversed  increasing/decreasing insert `timestamp`'s value
     * @param deleted   if deleted is false, set `tag` to 0; otherwise set `tag` to 1
     * @return
     */
    static Block prepareBlockWithTso(
        Int64 pk,
        size_t ts_beg,
        size_t ts_end,
        bool reversed = false,
        bool deleted = false)
    {
        Block block;
        const size_t num_rows = (ts_end - ts_beg);
        // int64 pk_col
        block.insert(
            DB::tests::createColumn<Int64>(std::vector<Int64>(num_rows, pk), pk_name, MutSup::extra_handle_id));
        // version_col
        block.insert(DB::tests::createColumn<UInt64>(
            createNumbers<UInt64>(ts_beg, ts_end, reversed),
            MutSup::version_column_name,
            MutSup::version_col_id));
        // tag_col
        block.insert(DB::tests::createColumn<UInt8>(
            std::vector<UInt64>(num_rows, deleted ? 1 : 0),
            MutSup::delmark_column_name,
            MutSup::delmark_col_id));
        return block;
    }

    /// prepare a row like this:
    /// {"pk":pk, "version":tso, "delete_mark":mark, "colname":value}
    static Block prepareOneRowBlock(
        Int64 pk,
        UInt64 tso,
        UInt8 mark,
        const String & colname,
        const String & value,
        bool is_common_handle,
        size_t rowkey_column_size,
        ColumnID column_id = 100)
    {
        Block block;
        const size_t num_rows = 1;
        if (is_common_handle)
        {
            Strings values{genMockCommonHandle(pk, rowkey_column_size)};
            block.insert(DB::tests::createColumn<String>(std::move(values), pk_name, MutSup::extra_handle_id));
        }
        else
        {
            // int64 pk_col
            block.insert(
                DB::tests::createColumn<Int64>(std::vector<Int64>(num_rows, pk), pk_name, MutSup::extra_handle_id));
        }
        // version_col
        block.insert(DB::tests::createColumn<UInt64>(
            std::vector<UInt64>(num_rows, tso),
            MutSup::version_column_name,
            MutSup::version_col_id));
        // tag_col
        block.insert(DB::tests::createColumn<UInt8>(
            std::vector<UInt64>(num_rows, mark),
            MutSup::delmark_column_name,
            MutSup::delmark_col_id));
        // string column
        block.insert(DB::tests::createColumn<String>(
            Strings{value},
            colname,
            /*column_id*/ column_id));
        return block;
    }

    static void verifyClusteredIndexValue(const String & value, Int64 ans, size_t rowkey_column_size)
    {
        size_t cursor = 0;
        size_t k = 0;
        for (; cursor < value.size() && k < rowkey_column_size; k++)
        {
            cursor++;
            Int64 i_value = DB::DecodeInt64(cursor, value);
            EXPECT_EQ(i_value, ans);
        }
        EXPECT_EQ(k, rowkey_column_size);
        EXPECT_EQ(cursor, value.size());
    }

    static RowKeyRange getRowKeyRangeForClusteredIndex(Int64 start, Int64 end, size_t rowkey_column_size)
    {
        RowKeyValue start_key
            = RowKeyValue(true, std::make_shared<String>(genMockCommonHandle(start, rowkey_column_size)));
        RowKeyValue end_key = RowKeyValue(true, std::make_shared<String>(genMockCommonHandle(end, rowkey_column_size)));
        return RowKeyRange(start_key, end_key, true, rowkey_column_size);
    }

    static Block prepareBlockWithIncreasingPKAndTs(size_t rows, Int64 start_pk, UInt64 start_ts)
    {
        Block block;
        // int64 pk_col
        block.insert(DB::tests::createColumn<Int64>(
            createNumbers<Int64>(start_pk, start_pk + rows),
            MutSup::extra_handle_column_name,
            MutSup::extra_handle_id));
        // version_col
        block.insert(DB::tests::createColumn<UInt64>(
            createNumbers<UInt64>(start_ts, start_ts + rows),
            MutSup::version_column_name,
            MutSup::version_col_id));
        // tag_col
        block.insert(DB::tests::createColumn<UInt8>(
            std::vector<UInt64>(rows, 0),
            MutSup::delmark_column_name,
            MutSup::delmark_col_id));
        return block;
    }

    static int getPseudoRandomNumber()
    {
        static int num = 0;
        return num++;
    }
};

} // namespace tests
} // namespace DM
} // namespace DB
