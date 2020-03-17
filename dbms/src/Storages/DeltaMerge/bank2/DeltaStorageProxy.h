//
// Created by linkmyth on 2020-03-17.
//
#ifndef CLICKHOUSE_DELTASTORAGEPROXY_H
#define CLICKHOUSE_DELTASTORAGEPROXY_H
#include <DataTypes/DataTypeString.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <test_utils/TiflashTestBasic.h>
#include <Poco/File.h>

#include <Core/Types.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <Storages/DeltaMerge/bank2/IDGenerator.h>
#include <cstddef>
#include <memory>
#include <iostream>

namespace DB {
    namespace DM {
        namespace tests {
            class DeltaStorageProxy {
            public:
                DeltaStorageProxy() : name{"bank2"}, col_balance_define{2, "balance", std::make_shared<DataTypeUInt64>()}
                {
                    // construct DeltaMergeStore
                    String path = DB::tests::TiFlashTestEnv::getTemporaryPath() + name;
                    Poco::File file(path);
                    if (file.exists())
                        file.remove(true);
                    context = std::make_unique<Context>(DMTestEnv::getContext());
                    auto table_column_defines = DMTestEnv::getDefaultColumns();
                    table_column_defines->emplace_back(col_balance_define);
                    ColumnDefine handle_column_define = (*table_column_defines)[0];
                    store = std::make_shared<DeltaMergeStore>(*context, path, "test", name, *table_column_defines, handle_column_define,
                                                                DeltaMergeStore::Settings());
                }
                void upsertRow(UInt64 id, UInt64 balance, UInt64 tso);

            public:
                void insertBalance(UInt64 id, UInt64 balance, UInt64 tso)
                {
                    upsertRow(id, balance, tso);
                }

                void updateBalance(UInt64 id, UInt64 balance, UInt64 tso)
                {
                    upsertRow(id, balance, tso);
                }

                UInt64 selectBalance(UInt64 id, UInt64 tso);

                UInt64 sumBalance(UInt64 begin, UInt64 end, UInt64 tso);

            public:
                void moveMoney(UInt64 from, UInt64 to, UInt64 num, UInt64 tso);

            private:
                String name;
                std::unique_ptr<Context> context;
                const ColumnDefine col_balance_define;
                DeltaMergeStorePtr store;

                static constexpr const char * pk_name = "_tidb_rowid";
            };
        }
    }
}


#endif //CLICKHOUSE_DELTASTORAGEPROXY_H
