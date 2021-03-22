#include <DataTypes/DataTypeString.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <Storages/DeltaMerge/tests/bank/DeltaMergeStoreProxy.h>
#include <Storages/DeltaMerge/tests/dm_basic_include.h>

#include <iostream>
#include <memory>
#include <thread>
#include "SimpleLockManager.h"


namespace DB
{
namespace DM
{
namespace tests
{
void moveMoney(DeltaMergeStoreProxy & proxy, UInt64 from, UInt64 to, UInt64 amount, UInt64 tso)
{
    UInt64 old_balance1 = proxy.selectBalance(from, tso);
    UInt64 old_balance2 = proxy.selectBalance(to, tso);
    proxy.moveMoney(from, to, amount, tso);
    UInt64 new_balance1 = proxy.selectBalance(from, tso);
    UInt64 new_balance2 = proxy.selectBalance(to, tso);
    if (old_balance1 >= amount)
    {
        EXPECT_EQ(old_balance1 - amount, new_balance1);
        EXPECT_EQ(old_balance2 + amount, new_balance2);
    }
    else
    {
        EXPECT_EQ(old_balance1, new_balance1);
        EXPECT_EQ(old_balance2, new_balance2);
    }

    std::cout << "Move money from " << std::to_string(from) << " to " << std::to_string(to) << " amount " << std::to_string(amount)
              << std::endl;
}
void work(DeltaMergeStoreProxy & proxy,
          SimpleLockManager &    manager,
          IDGenerator &          tso_gen,
          IDGenerator &          trans_id_gen,
          UInt64                 max_id,
          UInt64                 try_num)
{
    for (size_t i = 0; i < try_num; i++)
    {
        UInt64 tid = trans_id_gen.get();
        UInt64 tso = tso_gen.get();
        UInt64 id1, id2;
        while (true)
        {
            id1 = std::rand() % max_id;
            id2 = std::rand() % max_id;
            if (id1 != id2)
            {
                break;
            }
        }
        UInt64 s_id = (id1 > id2) ? id2 : id1;
        UInt64 b_id = (id1 > id2) ? id1 : id2;
        if (!manager.writeLock(s_id, tid, tso))
            return;
        if (!manager.writeLock(b_id, tid, tso))
        {
            manager.writeUnlock(s_id, tid);
            return;
        }
        UInt64 amount    = std::rand() % 100;
        int    direction = std::rand() % 2;
        if (direction == 0)
        {
            moveMoney(proxy, s_id, b_id, amount, tso);
        }
        else
        {
            moveMoney(proxy, b_id, s_id, amount, tso);
        }
        manager.writeUnlock(s_id, tid);
        manager.writeUnlock(b_id, tid);
    }
}

void verify(DeltaMergeStoreProxy & proxy,
            SimpleLockManager &    manager,
            IDGenerator &          tso_gen,
            IDGenerator &          trans_id_gen,
            UInt64                 max_id,
            UInt64                 total,
            UInt64                 try_num)
{
    for (size_t i = 0; i < try_num; i++)
    {
        UInt64 tid = trans_id_gen.get();
        UInt64 tso = tso_gen.get();

        for (UInt64 id = 0; id < max_id; id++)
        {
            manager.readLock(id, tid, tso);
        }

        EXPECT_EQ(proxy.sumBalance(0, max_id, tso), total);
        if (proxy.sumBalance(0, max_id, tso) != total)
        {
            std::cout << "Sum balance is wrong" << std::endl;
            throw std::exception();
        }

        std::cout << proxy.sumBalance(0, max_id, tso) << std::endl;

        for (UInt64 id = 0; id < max_id; id++)
        {
            manager.readUnlock(id, tid);
        }
    }
}

void run_bank(UInt64 account, UInt64 initial_balance, UInt64 worker_count, UInt64 try_num)
{
    DeltaMergeStoreProxy proxy;
    SimpleLockManager    manager;
    IDGenerator          tso_gen;
    IDGenerator          trans_id_gen;

    UInt64 start = 0;
    UInt64 end   = account;
    UInt64 total = (end - start) * initial_balance;

    for (UInt64 id = start; id < end; id++)
    {
        auto tso = tso_gen.get();
        proxy.insertBalance(id, initial_balance, tso);
    }

    std::vector<std::thread> workers;
    workers.resize(worker_count);

    for (size_t i = 0; i < worker_count; i++)
    {
        workers[i] = std::thread{work, std::ref(proxy), std::ref(manager), std::ref(tso_gen), std::ref(trans_id_gen), end, try_num};
    }

    std::thread verify_thread{verify, std::ref(proxy), std::ref(manager), std::ref(tso_gen), std::ref(trans_id_gen), end, total, try_num};

    for (size_t i = 0; i < worker_count; i++)
    {
        workers[i].join();
    }
    verify_thread.join();

    std::cout << "Last Verify\n";
    std::cout << proxy.sumBalance(0, end, UINT64_MAX) << std::endl;

    std::cout << "Complete\n";
}
} // namespace tests
} // namespace DM
} // namespace DB

int main(int argc, char * argv[])
{
    if (argc != 5)
    {
        std::cout << "Usage: <cmd> account balance worker try_num" << std::endl;
        return 1;
    }
    UInt64 account = std::stoul(argv[1]);
    UInt64 balance = std::stoul(argv[2]);
    UInt64 worker  = std::stoul(argv[3]);
    UInt64 try_num = std::stoul(argv[4]);
    DB::DM::tests::run_bank(account, balance, worker, try_num);
    return 0;
}
