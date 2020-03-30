#pragma once

#include <Core/Types.h>
#include <chrono>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace DB
{
namespace DM
{
namespace tests
{

enum class LockType
{
    READ  = 0,
    WRITE = 1
};

struct SimpleLock
{
    UInt64   transaction_id;
    UInt64   tso;
    LockType type;
};

using SimpleLocks = std::vector<SimpleLock>;

class SimpleLockManager
{
public:
    void readLock(UInt64 id, UInt64 transaction_id, UInt64 tso)
    {
        std::unique_lock<std::mutex> latch{mutex};
        if (lock_map.find(id) == lock_map.end())
        {
            lock_map.emplace(std::piecewise_construct, std::make_tuple(id), std::make_tuple());
        }
        latch.unlock();
        while (true)
        {
            latch.lock();
            if (!isWriteLocked(id, tso))
            {
                auto &     locks = lock_map[id];
                SimpleLock l{transaction_id, tso, LockType::READ};
                locks.emplace_back(l);
                latch.unlock();
                return;
            }
            latch.unlock();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    bool writeLock(UInt64 id, UInt64 transaction_id, UInt64 tso)
    {
        std::unique_lock<std::mutex> latch{mutex};
        if (lock_map.find(id) == lock_map.end())
        {
            lock_map.emplace(std::piecewise_construct, std::make_tuple(id), std::make_tuple());
        }
        if (isWriteLocked(id, UINT64_MAX))
        {
            latch.unlock();
            return false;
        }
        latch.unlock();
        while (true)
        {
            latch.lock();
            if (isWriteLocked(id, UINT64_MAX))
            {
                latch.unlock();
                return false;
            }
            if (!isReadLocked(id, tso))
            {
                auto &     locks = lock_map[id];
                SimpleLock l{transaction_id, tso, LockType::WRITE};
                locks.emplace_back(l);
                latch.unlock();
                return true;
            }
            latch.unlock();
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    void readUnlock(UInt64 id, UInt64 transaction_id)
    {
        std::lock_guard<std::mutex> guard{mutex};
        auto &                      locks = lock_map[id];
        size_t                      index = UINT64_MAX;
        for (size_t i = 0; i < locks.size(); i++)
        {
            if (locks[i].transaction_id == transaction_id && locks[i].type == LockType::READ)
            {
                index = i;
            }
        }
        if (index != UINT64_MAX)
        {
            locks.erase(locks.begin() + index, locks.begin() + index + 1);
        }
        else
        {
            std::cout << std::to_string(id) << " cannot find read lock for transaction " << std::to_string(transaction_id) << std::endl;
            throw std::exception();
        }
    }

    void writeUnlock(UInt64 id, UInt64 transaction_id)
    {
        std::lock_guard<std::mutex> guard{mutex};
        auto &                      locks = lock_map[id];
        size_t                      index = UINT64_MAX;
        for (size_t i = 0; i < locks.size(); i++)
        {
            if (locks[i].transaction_id == transaction_id && locks[i].type == LockType::WRITE)
            {
                index = i;
            }
        }
        if (index != UINT64_MAX)
        {
            locks.erase(locks.begin() + index, locks.begin() + index + 1);
        }
        else
        {
            std::cout << std::to_string(id) << " cannot find write lock for transaction " << std::to_string(transaction_id) << std::endl;
            throw std::exception();
        }
    }

    // test whether there is read lock after tso
    bool isReadLocked(UInt64 id, UInt64 tso)
    {
        auto & locks = lock_map[id];
        for (auto & lock : locks)
        {
            if (lock.type == LockType::READ && lock.tso >= tso)
            {
                return true;
            }
        }
        return false;
    }

    // test whether there is write lock before or equal tso
    bool isWriteLocked(UInt64 id, UInt64 tso)
    {
        auto & locks = lock_map[id];
        for (auto & lock : locks)
        {
            if (lock.type == LockType::WRITE && lock.tso <= tso)
            {
                return true;
            }
        }
        return false;
    }

private:
    std::mutex                              mutex;
    std::unordered_map<UInt64, SimpleLocks> lock_map;
};
} // namespace tests
} // namespace DM
} // namespace DB
