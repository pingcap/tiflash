#pragma once

#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Range.h>
#include <TestUtils/TiFlashTestBasic.h>

namespace DB
{
namespace DM
{
namespace tests
{

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
    return ::testing::internal::EqFailure(lhs_expr, rhs_expr, lhs.toDebugString(), rhs.toDebugString(), false);
}
#define ASSERT_RANGE_EQ(val1, val2) ASSERT_PRED_FORMAT2(::DB::DM::tests::HandleRangeCompare, val1, val2)
#define EXPECT_RANGE_EQ(val1, val2) EXPECT_PRED_FORMAT2(::DB::DM::tests::HandleRangeCompare, val1, val2)
#define GET_GTEST_FULL_NAME                                                                     \
    (String() + ::testing::UnitTest::GetInstance()->current_test_info()->test_case_name() + "." \
     + ::testing::UnitTest::GetInstance()->current_test_info()->name())

class DMTestEnv
{
public:
    static Context getContext(const ::DB::Settings & settings = DB::Settings())
    {
        return ::DB::tests::TiFlashTestEnv::getContext(settings);
    }

    static constexpr const char * pk_name = "_tidb_rowid";

    static constexpr const char * PK_NAME_PK_IS_HANDLE = "id";

    enum class PkType
    {
        // If the primary key is composed of multiple columns and non-clustered-index,
        // or users don't define the primary key, TiDB will add a hidden "_tidb_rowid" column
        // as the handle column
        HiddenTiDBRowID,

        // // Common handle for clustered-index since 5.0.0
        // CommonHandle,

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
        // case PkType::CommonHandle:
        //     return "CommonHandle";
        case PkType::PkIsHandleInt64:
            return "PkIsHandleInt64";
        case PkType::PkIsHandleInt32:
            return "PkIsHandleInt32";
        }
        return "<unknown>";
    }

    static ColumnDefinesPtr getDefaultColumns(PkType pk_type = PkType::HiddenTiDBRowID)
    {
        // Return [handle, ver, del] column defines
        ColumnDefinesPtr columns = std::make_shared<ColumnDefines>();
        switch (pk_type)
        {
        case PkType::HiddenTiDBRowID:
            columns->emplace_back(getExtraHandleColumnDefine());
            break;
        // case PkType::CommonHandle:
        //     columns->emplace_back(getExtraHandleColumnDefine(/*is_common_handle=*/true));
        //     break;
        case PkType::PkIsHandleInt64:
            columns->emplace_back(ColumnDefine{2, PK_NAME_PK_IS_HANDLE, EXTRA_HANDLE_COLUMN_TYPE});
            break;
        case PkType::PkIsHandleInt32:
            columns->emplace_back(ColumnDefine{2, PK_NAME_PK_IS_HANDLE, DataTypeFactory::instance().get("Int32")});
            break;
        default:
            throw Exception("Unknown pk type for test");
        }
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
                                         UInt64         tso       = 2,
                                         const String & pk_name_  = pk_name,
                                         ColumnID       pk_col_id = EXTRA_HANDLE_COLUMN_ID,
                                         DataTypePtr    pk_type   = EXTRA_HANDLE_COLUMN_TYPE)
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
                    if (!reversed)
                    {
                        field = Int64(beg + i);
                    }
                    else
                    {
                        field = Int64(end - 1 - i);
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
    static Block prepareOneRowBlock(Int64 pk, UInt64 tso, UInt8 mark, const String & colname, const String & value)
    {
        Block        block;
        const size_t num_rows = 1;
        {
            ColumnWithTypeAndName col1(EXTRA_HANDLE_COLUMN_TYPE, pk_name);
            {
                IColumn::MutablePtr m_col = col1.type->createColumn();
                // insert form large to small
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
};


} // namespace tests
} // namespace DM
} // namespace DB
