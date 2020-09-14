#pragma once

#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Range.h>
#include <Storages/DeltaMerge/RowKeyRange.h>
#include <test_utils/TiflashTestBasic.h>

namespace DB
{
namespace DM
{
namespace tests
{

#define GET_REGION_RANGE(start, end, table_id) RowKeyRange::fromHandleRange(::DB::DM::HandleRange((start), (end))).toRegionRange((table_id))

// Add this so that we can call typeFromString under namespace DB::DM::tests
using DB::tests::typeFromString;

/// helper functions for comparing HandleRange
inline ::testing::AssertionResult HandleRangeCompare(const char *        lhs_expr,
                                                     const char *        rhs_expr, //
                                                     const HandleRange & lhs,
                                                     const HandleRange & rhs)
{
    if (lhs == rhs)
        return ::testing::AssertionSuccess();
    else
        return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.toString(), rhs.toString(), false);
}
/// helper functions for comparing HandleRange
inline ::testing::AssertionResult RowKeyRangeCompare(const char *        lhs_expr,
                                                     const char *        rhs_expr, //
                                                     const RowKeyRange & lhs,
                                                     const RowKeyRange & rhs)
{
    if (lhs == rhs)
        return ::testing::AssertionSuccess();
    else
        return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.toString(), rhs.toString(), false);
}
#define ASSERT_RANGE_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::DM::tests::HandleRangeCompare, val1, val2)
#define ASSERT_ROWKEY_RANGE_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::DM::tests::RowKeyRangeCompare, val1, val2)
#define EXPECT_RANGE_EQ(val1, val2) EXPECT_PRED_FORMAT2(::DB::DM::tests::HandleRangeCompare, val1, val2)
#define GET_GTEST_FULL_NAME                                                                     \
    (String() + ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name() + "." \
     + ::testing::UnitTest::GetInstance()->current_test_info()->name())

class DMTestEnv
{
public:
    static Context & getContext(const ::DB::Settings & settings = DB::Settings())
    {
        return ::DB::tests::TiFlashTestEnv::getContext(settings);
    }

    static constexpr const char * pk_name = "_tidb_rowid";

    static ColumnDefinesPtr getDefaultColumns(bool is_common_handle = false)
    {
        ColumnDefinesPtr columns = std::make_shared<ColumnDefines>();
        columns->emplace_back(getExtraHandleColumnDefine(is_common_handle));
        columns->emplace_back(getVersionColumnDefine());
        columns->emplace_back(getTagColumnDefine());
        return columns;
    }

    /**
     * Create a simple block with 3 columns:
     *   * `pk` - Int64 / `version` / `tag`
     * @param beg       `pk`'s value begin
     * @param end       `pk`'s value end (not included)
     * @param reversed  increasing/decreasing insert `pk`'s value
     * @return
     */
    static Block prepareSimpleWriteBlock(size_t         beg,
                                         size_t         end,
                                         bool           reversed,
                                         UInt64         tso                = 2,
                                         const String & pk_name_           = pk_name,
                                         ColumnID       pk_col_id          = EXTRA_HANDLE_COLUMN_ID,
                                         DataTypePtr    pk_type            = EXTRA_HANDLE_COLUMN_INT_TYPE,
                                         bool           is_common_handle   = false,
                                         size_t         rowkey_column_size = 1)
    {
        Block        block;
        const size_t num_rows = (end - beg);
        {
            {
                ColumnWithTypeAndName col1({}, pk_type, pk_name_, pk_col_id);
                IColumn::MutablePtr   m_col = col1.type->createColumn();
                // insert form large to small
                for (size_t i = 0; i < num_rows; i++)
                {
                    Field field;
                    if (is_common_handle)
                    {
                        Int64             value = reversed ? end - 1 - i : beg + i;
                        std::stringstream ss;
                        for (size_t index = 0; index < rowkey_column_size; index++)
                        {
                            ss << TiDB::CodecFlagInt;
                            ::DB::EncodeInt64(value, ss);
                        }
                        field = ss.str();
                    }
                    else
                    {
                        if (!reversed)
                        {
                            field = Int64(beg + i);
                        }
                        else
                        {
                            field = Int64(end - 1 - i);
                        }
                    }
                    m_col->insert(field);
                }
                col1.column = std::move(m_col);
                block.insert(col1);
            }

            {
                ColumnWithTypeAndName version_col({}, VERSION_COLUMN_TYPE, VERSION_COLUMN_NAME, VERSION_COLUMN_ID);
                IColumn::MutablePtr   m_col = version_col.type->createColumn();
                for (size_t i = 0; i < num_rows; ++i)
                {
                    Field field = tso;
                    m_col->insert(field);
                }
                version_col.column = std::move(m_col);
                block.insert(version_col);
            }

            {
                ColumnWithTypeAndName tag_col({}, TAG_COLUMN_TYPE, TAG_COLUMN_NAME, TAG_COLUMN_ID);
                IColumn::MutablePtr   m_col       = tag_col.type->createColumn();
                auto &                column_data = typeid_cast<ColumnVector<UInt8> &>(*m_col).getData();
                column_data.resize(num_rows);
                for (size_t i = 0; i < num_rows; ++i)
                {
                    column_data[i] = 0;
                }
                tag_col.column = std::move(m_col);
                block.insert(tag_col);
            }
        }
        return block;
    }

    /**
     * Create a simple block with 3 columns:
     *   * `pk` - Int64 / `version` / `tag`
     * @param pk        `pk`'s value
     * @param tso_beg   `tso`'s value begin
     * @param tso_end   `tso`'s value end (not included)
     * @return
     */
    static Block prepareBlockWithIncreasingTso(Int64 pk, size_t tso_beg, size_t tso_end)
    {
        Block        block;
        const size_t num_rows = (tso_end - tso_beg);
        {
            ColumnWithTypeAndName col1(std::make_shared<DataTypeInt64>(), pk_name);
            {
                IColumn::MutablePtr m_col = col1.type->createColumn();
                // insert form large to small
                for (size_t i = 0; i < num_rows; i++)
                {
                    Field field = Int64(pk);
                    m_col->insert(field);
                }
                col1.column = std::move(m_col);
            }
            block.insert(col1);

            ColumnWithTypeAndName version_col(VERSION_COLUMN_TYPE, VERSION_COLUMN_NAME);
            {
                IColumn::MutablePtr m_col = version_col.type->createColumn();
                for (size_t i = 0; i < num_rows; ++i)
                {
                    Field field = Int64(tso_beg + i);
                    m_col->insert(field);
                }
                version_col.column = std::move(m_col);
            }
            block.insert(version_col);

            ColumnWithTypeAndName tag_col(TAG_COLUMN_TYPE, TAG_COLUMN_NAME);
            {
                IColumn::MutablePtr m_col       = tag_col.type->createColumn();
                auto &              column_data = typeid_cast<ColumnVector<UInt8> &>(*m_col).getData();
                column_data.resize(num_rows);
                for (size_t i = 0; i < num_rows; ++i)
                {
                    column_data[i] = 0;
                }
                tag_col.column = std::move(m_col);
            }
            block.insert(tag_col);
        }
        return block;
    }

    /// prepare a row like this:
    /// {"pk":pk, "version":tso, "delete_mark":mark, "colname":value}
    static Block prepareOneRowBlock(
        Int64 pk, UInt64 tso, UInt8 mark, const String & colname, const String & value, bool is_common_handle, size_t rowkey_column_size)
    {
        Block        block;
        const size_t num_rows = 1;
        {
            ColumnWithTypeAndName col1(is_common_handle ? EXTRA_HANDLE_COLUMN_STRING_TYPE : EXTRA_HANDLE_COLUMN_INT_TYPE, pk_name);
            {
                IColumn::MutablePtr m_col = col1.type->createColumn();
                // insert form large to small
                if (is_common_handle)
                {
                    Field             field;
                    std::stringstream ss;
                    for (size_t index = 0; index < rowkey_column_size; index++)
                    {
                        ss << TiDB::CodecFlagInt;
                        ::DB::EncodeInt64(pk, ss);
                    }
                    field = ss.str();
                    m_col->insert(field);
                }
                else
                    m_col->insert(pk);
                col1.column = std::move(m_col);
            }
            block.insert(col1);

            ColumnWithTypeAndName version_col(VERSION_COLUMN_TYPE, VERSION_COLUMN_NAME);
            {
                IColumn::MutablePtr m_col = version_col.type->createColumn();
                m_col->insert(tso);
                version_col.column = std::move(m_col);
            }
            block.insert(version_col);

            ColumnWithTypeAndName tag_col(TAG_COLUMN_TYPE, TAG_COLUMN_NAME);
            {
                IColumn::MutablePtr m_col       = tag_col.type->createColumn();
                auto &              column_data = typeid_cast<ColumnVector<UInt8> &>(*m_col).getData();
                column_data.resize(num_rows);
                column_data[0] = mark;
                tag_col.column = std::move(m_col);
            }
            block.insert(tag_col);

            ColumnWithTypeAndName str_col(DataTypeFactory::instance().get("String"), colname);
            {
                IColumn::MutablePtr m_col = str_col.type->createColumn();
                m_col->insert(value);
                str_col.column = std::move(m_col);
            }
            block.insert(str_col);
        }
        return block;
    }

    static void verifyClusteredIndexValue(const String & value, Int64 ans, size_t rowkey_column_size)
    {
        size_t cursor = 0;
        size_t k      = 0;
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
        std::stringstream ss;
        for (size_t i = 0; i < rowkey_column_size; i++)
        {
            ss << TiDB::CodecFlagInt;
            EncodeInt64(start, ss);
        }
        RowKeyValue start_key = RowKeyValue(true, std::make_shared<String>(ss.str()));
        ss.str("");
        for (size_t i = 0; i < rowkey_column_size; i++)
        {
            ss << TiDB::CodecFlagInt;
            EncodeInt64(end, ss);
        }
        RowKeyValue end_key = RowKeyValue(true, std::make_shared<String>(ss.str()));
        return RowKeyRange(start_key, end_key, true, rowkey_column_size);
    }
};


} // namespace tests
} // namespace DM
} // namespace DB
