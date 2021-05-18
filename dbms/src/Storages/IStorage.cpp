#include <Storages/IStorage.h>


namespace DB
{

namespace ErrorCodes
{
extern const int DEADLOCK_AVOIDED;
extern const int TABLE_IS_DROPPED;
} // namespace ErrorCodes


RWLock::LockHolder IStorage::tryLockTimed(
    const RWLockPtr & rwlock, RWLock::Type type, const String & query_id, const std::chrono::milliseconds & acquire_timeout) const
{
    auto lock_holder = rwlock->getLock(type, query_id, acquire_timeout);
    if (!lock_holder)
    {
        const String type_str = type == RWLock::Type::Read ? "READ" : "WRITE";
        throw Exception(type_str + " locking attempt on \"" + getTableName() + "\" has timed out! ("
                + std::to_string(acquire_timeout.count())
                + "ms) "
                  "Possible deadlock avoided. Client should retry.",
            ErrorCodes::DEADLOCK_AVOIDED);
    }
    return lock_holder;
}

TableLockHolder IStorage::lockForShare(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(drop_lock, RWLock::Read, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    return result;
}

TableLockHolder IStorage::lockForAlter(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableLockHolder result = tryLockTimed(alter_lock, RWLock::Write, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    return result;
}

TableStructureLockHolder IStorage::lockStructureForShare(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableStructureLockHolder result;
    result.alter_lock = tryLockTimed(alter_lock, RWLock::Read, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    result.drop_lock = tryLockTimed(drop_lock, RWLock::Read, query_id, acquire_timeout);

    return result;
}

TableExclusiveLockHolder IStorage::lockExclusively(const String & query_id, const std::chrono::milliseconds & acquire_timeout)
{
    TableExclusiveLockHolder result;
    result.alter_lock = tryLockTimed(alter_lock, RWLock::Write, query_id, acquire_timeout);

    if (is_dropped)
        throw Exception("Table is dropped", ErrorCodes::TABLE_IS_DROPPED);

    result.drop_lock = tryLockTimed(drop_lock, RWLock::Write, query_id, acquire_timeout);

    return result;
}
} // namespace DB
