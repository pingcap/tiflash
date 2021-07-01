#pragma once

#include <Core/Types.h>
#include <DataTypes/DataTypeString.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <Storages/DeltaMerge/tests/stress/SimpleDB.h>
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

template <typename T>
class IDGenerator
{
public:
    IDGenerator(T t_) : t(t_) {}
    std::vector<T> get(Int32 count)
    {
        T              start = t.fetch_add(count);
        std::vector<T> v(count);
        for (int i = 0; i < count; i++)
        {
            v[i] = start + i;
        }
        return v;
    }

    T get() { return t.fetch_add(1); }

private:
    std::atomic<T> t;
};

class DeltaMergeStoreProxy
{
public:
    DeltaMergeStoreProxy()
        : name{"stress"},
          col_balance_define{2, "balance", std::make_shared<DataTypeUInt64>()},
          col_random_define{3, "random_text", std::make_shared<DataTypeString>()},
          log(&Poco::Logger::get("DeltaMergeStoreProxy"))
    {
        // construct DeltaMergeStore
        String     path = DB::tests::TiFlashTestEnv::getTemporaryPath() + name;
        Poco::File file(path);
        if (file.exists())
            file.remove(true);
        context                   = std::make_unique<Context>(DMTestEnv::getContext());
        auto table_column_defines = DMTestEnv::getDefaultColumns();
        table_column_defines->emplace_back(col_balance_define);
        table_column_defines->emplace_back(col_random_define);
        ColumnDefine handle_column_define = (*table_column_defines)[0];
        store                             = std::make_shared<DeltaMergeStore>(
            *context, true, "test", name, *table_column_defines, handle_column_define, false, 1, DeltaMergeStore::Settings());
    }

    void   write(const std::vector<Int64> & ids);
    UInt64 countRows();

    void genDataMultiThread(UInt64 count, Int32 concurrency);
    void genData(UInt64 count);

    void readDataMultiThread(Int32 concurrency);

private:
    void genBlock(Block & block, const std::vector<Int64> & ids);

    String                   name;
    std::unique_ptr<Context> context;
    const ColumnDefine       col_balance_define;
    const ColumnDefine       col_random_define;
    DeltaMergeStorePtr       store;

    SimpleDB db;

    const String pk_name = EXTRA_HANDLE_COLUMN_NAME;

    std::mutex mutex;

    Poco::Logger * log;
};
} // namespace tests
} // namespace DM
} // namespace DB
