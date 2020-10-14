#include <Common/FailPoint.h>

#include <boost/core/noncopyable.hpp>
#include <condition_variable>
#include <mutex>

namespace DB
{
std::unordered_map<String, std::shared_ptr<FailPointChannel>> FailPointHelper::fail_point_wait_channels;

#define APPLY_FOR_FAILPOINTS(M)                              \
    M(exception_between_drop_meta_and_data)                  \
    M(exception_between_alter_data_and_meta)                 \
    M(exception_drop_table_during_remove_meta)               \
    M(exception_between_rename_table_data_and_metadata);     \
    M(exception_between_create_database_meta_and_directory); \
    M(exception_before_rename_table_old_meta_removed);       \
    M(exception_after_step_1_in_exchange_partition)          \
    M(exception_before_step_2_rename_in_exchange_partition)  \
    M(exception_after_step_2_in_exchange_partition)          \
    M(exception_before_step_3_rename_in_exchange_partition)  \
    M(exception_after_step_3_in_exchange_partition)          \
    M(region_exception_after_read_from_storage_some_error)   \
    M(region_exception_after_read_from_storage_all_error)    \
    M(exception_before_dmfile_remove_encryption)             \
    M(exception_before_dmfile_remove_from_disk)

#define APPLY_FOR_FAILPOINTS_WITH_CHANNEL(M) M(pause_after_learner_read)

namespace FailPoints
{
#define M(NAME) extern const char NAME[] = #NAME "";
APPLY_FOR_FAILPOINTS(M)
APPLY_FOR_FAILPOINTS_WITH_CHANNEL(M)
#undef M
} // namespace FailPoints

#ifdef FIU_ENABLE
class FailPointChannel : private boost::noncopyable
{
public:
    // wake up all waiting threads when destroy
    ~FailPointChannel() { cv.notify_all(); }

    void wait()
    {
        std::unique_lock lock(m);
        cv.wait(lock);
    }

private:
    std::mutex m;
    std::condition_variable cv;
};

void FailPointHelper::enableFailPoint(const String & fail_point_name)
{
#define M(NAME)                                                                                             \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, FIU_ONETIME);                                              \
        return;                                                                                             \
    }

    APPLY_FOR_FAILPOINTS(M)
#undef M

#define M(NAME)                                                                                             \
    if (fail_point_name == FailPoints::NAME)                                                                \
    {                                                                                                       \
        /* FIU_ONETIME -- Only fail once; the point of failure will be automatically disabled afterwards.*/ \
        fiu_enable(FailPoints::NAME, 1, nullptr, FIU_ONETIME);                                              \
        fail_point_wait_channels.try_emplace(FailPoints::NAME, std::make_shared<FailPointChannel>());       \
        return;                                                                                             \
    }

    APPLY_FOR_FAILPOINTS_WITH_CHANNEL(M)
#undef M
    throw Exception("Cannot find fail point " + fail_point_name, ErrorCodes::FAIL_POINT_ERROR);
}

void FailPointHelper::disableFailPoint(const String & fail_point_name)
{
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
        fail_point_wait_channels.erase(iter);
    fiu_disable(fail_point_name.c_str());
}

void FailPointHelper::wait(const String & fail_point_name)
{
    if (auto iter = fail_point_wait_channels.find(fail_point_name); iter == fail_point_wait_channels.end())
        throw Exception("Can not find channel for fail point" + fail_point_name);
    else
    {
        auto ptr = iter->second;
        ptr->wait();
    }
}
#else
class FailPointChannel
{
};

void FailPointHelper::enableFailPoint(const String &) {}

void FailPointHelper::disableFailPoint(const String &) {}

void FailPointHelper::wait(const String &) {}
#endif

} // namespace DB
