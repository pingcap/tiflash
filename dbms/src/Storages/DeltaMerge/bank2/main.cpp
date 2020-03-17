#include <DataTypes/DataTypeString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/Filter/RSOperator.h>
#include <test_utils/TiflashTestBasic.h>

#include <Storages/DeltaMerge/tests/dm_basic_include.h>
#include <Storages/DeltaMerge/bank2/DeltaStorageProxy.h>

#include <memory>
#include <iostream>
#include <thread>
#include "SimpleLockManager.h"


namespace DB
{
namespace DM
{
namespace tests
{
void work(DeltaStorageProxy & proxy, SimpleLockManager & manager, IDGenerator & tso_gen, IDGenerator & trans_id_gen, UInt64 max_id)
{
    for(size_t i = 0; i < 100; i++) {
        UInt64 tid = trans_id_gen.get();
        UInt64 tso = tso_gen.get();
        UInt64 id1, id2;
        while (true) {
            id1 = std::rand() % max_id;
            id2 = std::rand() % max_id;
            if (id1 != id2) {
                break;
            }
        }
        UInt64 s_id = (id1 > id2) ? id2 : id1;
        UInt64 b_id = (id1 > id2) ? id1 : id2;
        manager.writeLock(s_id, tid, tso);
        manager.writeLock(b_id, tid, tso);
        UInt64 amount = std::rand() % 100;
        int direction = std::rand() % 2;
        if (direction == 0) {
            UInt64 old_balance1 = proxy.selectBalance(s_id, tso);
            UInt64 old_balance2 = proxy.selectBalance(b_id, tso);
            proxy.moveMoney(s_id, b_id, amount, tso);
            UInt64 new_balance1 = proxy.selectBalance(s_id, tso);
            UInt64 new_balance2 = proxy.selectBalance(b_id, tso);
            if (old_balance1 > amount) {
                EXPECT_EQ(old_balance1 - amount, new_balance1);
                EXPECT_EQ(old_balance2 + amount, new_balance2);
            } else {
                EXPECT_EQ(old_balance1, new_balance1);
                EXPECT_EQ(old_balance2, new_balance2);
            }

            std::cout << "move money from " << std::to_string(s_id) << " to " << std::to_string(b_id) << " amount " << std::to_string(amount) << std::endl;
        } else {
            UInt64 old_balance1 = proxy.selectBalance(s_id, tso);
            UInt64 old_balance2 = proxy.selectBalance(b_id, tso);
            proxy.moveMoney(b_id, s_id, amount, tso);
            UInt64 new_balance1 = proxy.selectBalance(s_id, tso);
            UInt64 new_balance2 = proxy.selectBalance(b_id, tso);
            if (old_balance2 > amount) {
                EXPECT_EQ(old_balance1 + amount, new_balance1);
                EXPECT_EQ(old_balance2 - amount, new_balance2);
            } else {
                EXPECT_EQ(old_balance1, new_balance1);
                EXPECT_EQ(old_balance2, new_balance2);
            }
            std::cout << "move money from " << std::to_string(b_id) << " to " << std::to_string(s_id) << " amount " << std::to_string(amount) << std::endl;
        }
        manager.writeUnlock(s_id, tid);
        manager.writeUnlock(b_id, tid);
    }
}

void verify(DeltaStorageProxy & proxy, SimpleLockManager & manager, IDGenerator & tso_gen, IDGenerator & trans_id_gen, UInt64 max_id, UInt64 total)
{
    for(size_t i = 0; i < 100; i++) {
        UInt64 tid = trans_id_gen.get();
        UInt64 tso = tso_gen.get();

        for (UInt64 id = 0; id < max_id; id++) {
            manager.readLock(id, tid, tso);
        }

        EXPECT_EQ(proxy.sumBalance(0, max_id, tso), total);

        std::cout << proxy.sumBalance(0, max_id, tso) << std::endl;

        for (UInt64 id = 0; id < max_id; id++) {
            manager.readUnlock(id, tid);
        }
    }
}

void run_bank2()
{
    DeltaStorageProxy proxy;
    SimpleLockManager manager;
    IDGenerator tso_gen;
    IDGenerator trans_id_gen;
    UInt64 start = 0;
    UInt64 end = 100;
    UInt64 initial_balance = 1000;
    UInt64 total = (end - start) * initial_balance;

    for (UInt64 id = start; id < end; id++)
    {
        auto tso = tso_gen.get();
        proxy.insertBalance(id, initial_balance, tso);
    }

    size_t worker_count = 2;
    std::vector<std::thread> workers;
    workers.resize(worker_count);

    for (size_t i = 0; i < worker_count; i++) {
        workers[i] = std::thread{work, std::ref(proxy), std::ref(manager), std::ref(tso_gen), std::ref(trans_id_gen), end};
    }

    std::thread verify_thread{verify, std::ref(proxy), std::ref(manager), std::ref(tso_gen), std::ref(trans_id_gen), end, total};

    for (size_t i = 0; i < worker_count; i++) {
        workers[i].join();
    }
    verify_thread.join();

    std::cout << "Last Verify\n";
    std::cout << proxy.sumBalance(0, end, UINT64_MAX) << std::endl;

    std::cout << "Complete\n";
}
}
}
}

int main()
{
    DB::DM::tests::run_bank2();
    return 0;
}
