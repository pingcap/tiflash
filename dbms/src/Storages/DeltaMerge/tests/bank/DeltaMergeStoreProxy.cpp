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

#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <Storages/DeltaMerge/tests/bank/DeltaMergeStoreProxy.h>

namespace DB
{
namespace DM
{
namespace tests
{
template <typename T>
void insertColumn(Block & block, const DataTypePtr & type, const String & name, Int64 col_id, T value)
{
    ColumnWithTypeAndName col({}, type, name, col_id);
    IColumn::MutablePtr m_col = col.type->createColumn();
    Field field = value;
    m_col->insert(field);
    col.column = std::move(m_col);
    block.insert(std::move(col));
}

void DeltaMergeStoreProxy::upsertRow(UInt64 id, UInt64 balance, UInt64 tso)
{
    std::lock_guard guard{mutex};
    Block block;

    insertColumn<Int64>(block, std::make_shared<DataTypeInt64>(), pk_name, EXTRA_HANDLE_COLUMN_ID, id);
    insertColumn<UInt64>(block, VERSION_COLUMN_TYPE, VERSION_COLUMN_NAME, VERSION_COLUMN_ID, tso);
    insertColumn<UInt64>(block, TAG_COLUMN_TYPE, TAG_COLUMN_NAME, TAG_COLUMN_ID, 0);
    insertColumn<UInt64>(block, col_balance_define.type, col_balance_define.name, col_balance_define.id, balance);

    store->write(*context, context->getSettingsRef(), std::move(block));
}

UInt64 DeltaMergeStoreProxy::selectBalance(UInt64 id, UInt64 tso)
{
    std::lock_guard guard{mutex};
    // read all columns from store
    const auto & columns = store->getTableColumns();
    BlockInputStreamPtr in = store->read(*context,
                                         context->getSettingsRef(),
                                         columns,
                                         {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                         /* num_streams= */ 1,
                                         /* max_version= */ tso,
                                         EMPTY_FILTER,
                                         /* expected_block_size= */ 1024)[0];

    bool found = false;
    size_t result = 0;
    size_t num_rows_read = 0;
    in->readPrefix();
    while (Block block = in->read())
    {
        num_rows_read += block.rows();
        ColumnWithTypeAndName col1 = block.getByName(pk_name);
        ColumnWithTypeAndName version_col = block.getByName(VERSION_COLUMN_NAME);
        ColumnWithTypeAndName tag_col = block.getByName(TAG_COLUMN_NAME);
        ColumnWithTypeAndName balance_col = block.getByName("balance");
        for (Int64 i = 0; i < Int64(col1.column->size()); ++i)
        {
            if (version_col.column->getUInt(i) > tso)
            {
                continue;
            }
            if ((col1.column->getUInt(i) == id) && ((*tag_col.column)[i].get<UInt8>() != 1))
            {
                if (!found)
                {
                    found = true;
                    result = balance_col.column->getUInt(i);
                }
            }
        }
    }
    in->readSuffix();
    EXPECT_EQ(found, true);
    UInt64 another_result = db.selectBalance(id, tso);
    if (another_result != result)
    {
        std::cout << "result between deltamerge and simpledb doesn't match" << std::endl;
        throw std::exception();
    }
    return result;
}

UInt64 DeltaMergeStoreProxy::sumBalance(UInt64 begin, UInt64 end, UInt64 tso)
{
    std::lock_guard guard{mutex};
    // read all columns from store
    const auto & columns = store->getTableColumns();
    BlockInputStreamPtr in = store->read(*context,
                                         context->getSettingsRef(),
                                         columns,
                                         {RowKeyRange::newAll(store->isCommonHandle(), store->getRowKeyColumnSize())},
                                         /* num_streams= */ 1,
                                         /* max_version= */ tso,
                                         EMPTY_FILTER,
                                         /* expected_block_size= */ 1024)[0];

    BoolVec found_status;
    std::vector<UInt64> result;
    found_status.resize(end - begin, false);
    result.resize(end - begin, 0);
    size_t num_rows_read = 0;
    in->readPrefix();
    while (Block block = in->read())
    {
        num_rows_read += block.rows();
        ColumnWithTypeAndName col1 = block.getByName(pk_name);
        ColumnWithTypeAndName version_col = block.getByName(VERSION_COLUMN_NAME);
        ColumnWithTypeAndName tag_col = block.getByName(TAG_COLUMN_NAME);
        ColumnWithTypeAndName balance_col = block.getByName("balance");
        for (Int64 i = 0; i < Int64(col1.column->size()); ++i)
        {
            if (version_col.column->getUInt(i) > tso)
            {
                continue;
            }
            UInt64 id = col1.column->getUInt(i);
            if ((id >= begin) && (id < end))
            {
                if ((*tag_col.column)[i].get<UInt8>() != 1)
                {
                    if (!found_status[id])
                    {
                        found_status[id] = true;
                        result[id] = balance_col.column->getUInt(i);
                    }
                }
            }
        }
    }
    in->readSuffix();
    UInt64 sum = 0;
    for (auto f : found_status)
        EXPECT_EQ(f, true);
    for (auto i : result)
        sum += i;
    UInt64 another_sum = db.sumBalance(begin, end, tso);
    if (another_sum != sum)
    {
        std::cout << "result between deltamerge and simpledb doesn't match" << std::endl;
        throw std::exception();
    }
    return sum;
}

void DeltaMergeStoreProxy::moveMoney(UInt64 from, UInt64 to, UInt64 num, UInt64 tso)
{
    UInt64 from_balance = selectBalance(from, tso);
    if (from_balance < num)
    {
        return;
    }
    UInt64 to_balance = selectBalance(to, tso);
    updateBalance(from, from_balance - num, tso);
    updateBalance(to, to_balance + num, tso);
}
} // namespace tests
} // namespace DM
} // namespace DB
