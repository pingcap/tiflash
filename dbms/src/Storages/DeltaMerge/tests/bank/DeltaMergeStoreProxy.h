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

#pragma once

#include <Core/Types.h>
#include <DataTypes/DataTypeString.h>
#include <Poco/File.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/bank/IDGenerator.h>
#include <Storages/DeltaMerge/tests/bank/SimpleDB.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <cstddef>
#include <iostream>
#include <memory>
#include <mutex>

namespace DB
{
namespace DM
{
namespace tests
{
class DeltaMergeStoreProxy
{
public:
    DeltaMergeStoreProxy()
        : name{"bank"}
        , col_balance_define{2, "balance", std::make_shared<DataTypeUInt64>()}
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
        store = std::make_shared<DeltaMergeStore>(
            *context,
            true,
            "test",
            name,
            *table_column_defines,
            handle_column_define,
            false,
            1,
            DeltaMergeStore::Settings());
    }
    void upsertRow(UInt64 id, UInt64 balance, UInt64 tso);

public:
    void insertBalance(UInt64 id, UInt64 balance, UInt64 tso)
    {
        db.insertBalance(id, balance, tso);
        upsertRow(id, balance, tso);
    }

    void updateBalance(UInt64 id, UInt64 balance, UInt64 tso)
    {
        db.updateBalance(id, balance, tso);
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

    SimpleDB db;

    const String pk_name = EXTRA_HANDLE_COLUMN_NAME;

    std::mutex mutex;
};
} // namespace tests
} // namespace DM
} // namespace DB
