//
// Created by linkmyth on 2020-03-17.
//

#include "DeltaStorageProxy.h"

namespace DB {
    namespace DM {
        namespace tests {

            void DeltaStorageProxy::upsertRow(UInt64 id, UInt64 balance, UInt64 tso) {
                Block block;
                {
                    ColumnWithTypeAndName col1({}, std::make_shared<DataTypeInt64>(), pk_name,
                                               EXTRA_HANDLE_COLUMN_ID);
                    IColumn::MutablePtr m_col = col1.type->createColumn();
                    Field field = Int64(id);
                    m_col->insert(field);
                    col1.column = std::move(m_col);
                    block.insert(col1);
                }

                {
                    ColumnWithTypeAndName version_col({}, VERSION_COLUMN_TYPE, VERSION_COLUMN_NAME,
                                                      VERSION_COLUMN_ID);
                    IColumn::MutablePtr m_col = version_col.type->createColumn();
                    Field field = tso;
                    m_col->insert(field);
                    version_col.column = std::move(m_col);
                    block.insert(version_col);
                }

                {
                    ColumnWithTypeAndName tag_col({}, TAG_COLUMN_TYPE, TAG_COLUMN_NAME, TAG_COLUMN_ID);
                    IColumn::MutablePtr m_col = tag_col.type->createColumn();
                    auto &column_data = typeid_cast<ColumnVector<UInt8> &>(*m_col).getData();
                    column_data.resize(1);
                    column_data[0] = 0;
                    tag_col.column = std::move(m_col);
                    block.insert(tag_col);
                }

                {
                    ColumnWithTypeAndName balance_col({}, col_balance_define.type, col_balance_define.name, col_balance_define.id);
                    IColumn::MutablePtr m_balance = balance_col.type->createColumn();
                    Field field = balance;
                    m_balance->insert(field);

                    balance_col.column = std::move(m_balance);
                    block.insert(std::move(balance_col));
                }

                store->write(*context, context->getSettingsRef(), block);
            }

            UInt64 DeltaStorageProxy::selectBalance(UInt64 id, UInt64 tso) {
                // read all columns from store
                const auto &        columns = store->getTableColumns();
                BlockInputStreamPtr in      = store->read(*context,
                                             context->getSettingsRef(),
                                             columns,
                                             {HandleRange::newAll()},
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
                        if (version_col.column->getUInt(i) > tso) {
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
                if (another_result != result) {
                    std::cout << "result between deltamerge and simpledb doesn't match" << std::endl;
                    throw std::exception();
                }
                return result;
            }

            UInt64 DeltaStorageProxy::sumBalance(UInt64 begin, UInt64 end, UInt64 tso) {
                // read all columns from store
                const auto &        columns = store->getTableColumns();
                BlockInputStreamPtr in      = store->read(*context,
                                             context->getSettingsRef(),
                                             columns,
                                             {HandleRange::newAll()},
                                             /* num_streams= */ 1,
                                             /* max_version= */ tso,
                                             EMPTY_FILTER,
                                             /* expected_block_size= */ 1024)[0];

                std::vector<bool> found_status;
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
                        if (version_col.column->getUInt(i) > tso) {
                            continue;
                        }
                        UInt64 id = col1.column->getUInt(i);
                        if ((id >= begin) && (id < end)) {
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
                if (another_sum != sum) {
                    std::cout << "result between deltamerge and simpledb doesn't match" << std::endl;
                    throw std::exception();
                }
                return sum;
            }

            void DeltaStorageProxy::moveMoney(UInt64 from, UInt64 to, UInt64 num, UInt64 tso) {
                UInt64 from_balance = selectBalance(from, tso);
                if (from_balance < num) {
                    return;
                }
                UInt64 to_balance = selectBalance(to, tso);
                updateBalance(from, from_balance - num, tso);
                updateBalance(to, to_balance + num, tso);
            }
        }
    }
}