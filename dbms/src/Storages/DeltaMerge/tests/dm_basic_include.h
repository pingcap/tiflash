#ifndef CLICKHOUSE_DM_BASIC_INCLUDE_H
#define CLICKHOUSE_DM_BASIC_INCLUDE_H

#include <gtest/gtest.h>
#include <Interpreters/Context.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Columns/ColumnVector.h>
#include <Common/typeid_cast.h>

namespace DB
{
namespace DM
{
namespace tests
{

class DMTestEnv
{
public:
    static Context getContext()
    {
        static Context context = DB::Context::createGlobal();
        return context;
    }

    static Block prepareSimpleWriteBlock(size_t beg, size_t end, bool reversed)
    {
        Block        block;
        const size_t num_rows = (end - beg);
        {
            ColumnWithTypeAndName col1(std::make_shared<DataTypeInt64>(), "pk");
            {
                IColumn::MutablePtr m_col = col1.type->createColumn();
                // insert form large to small
                for (size_t i = 0; i < num_rows; i++)
                {
                    Field field;
                    if (!reversed) {
                        field = Int64(beg + i);
                    } else {
                        field = Int64(end - 1 - i);
                    }
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
                    Field field = UInt64(2);
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
};



} // namespace tests
} // namespace DM
} // namespace DB


#endif //CLICKHOUSE_DM_BASIC_INCLUDE_H
