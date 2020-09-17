#pragma once
#include <Common/Exception.h>
#include <Core/Types.h>
#include <fiu-control.h>
#include <fiu-local.h>
#include <fiu.h>

#include <boost/core/noncopyable.hpp>
#include <condition_variable>
#include <mutex>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
extern const int FAIL_POINT_ERROR;
};

#define FAIL_POINT_REGISTER(name) static constexpr char name[] = #name "";

#define FAIL_POINT_ENABLE(trigger, name) \
    else if (trigger == name) { fiu_enable(name, 1, nullptr, FIU_ONETIME); }
#define FAIL_POINT_ENABLE_WITH_CHANNEL(trigger, name)                                     \
    else if (trigger == name)                                                             \
    {                                                                                     \
        fiu_enable(name, 1, nullptr, FIU_ONETIME);                                        \
        fail_point_wait_channels.try_emplace(name, std::make_shared<FailPointChannel>()); \
    }

FAIL_POINT_REGISTER(exception_between_drop_meta_and_data)
FAIL_POINT_REGISTER(exception_between_alter_data_and_meta)
FAIL_POINT_REGISTER(exception_drop_table_during_remove_meta)
FAIL_POINT_REGISTER(exception_between_rename_table_data_and_metadata);
FAIL_POINT_REGISTER(exception_between_create_database_meta_and_directory);
FAIL_POINT_REGISTER(exception_before_rename_table_old_meta_removed);
FAIL_POINT_REGISTER(exception_after_step_1_in_exchange_partition)
FAIL_POINT_REGISTER(exception_before_step_2_rename_in_exchange_partition)
FAIL_POINT_REGISTER(exception_after_step_2_in_exchange_partition)
FAIL_POINT_REGISTER(exception_before_step_3_rename_in_exchange_partition)
FAIL_POINT_REGISTER(exception_after_step_3_in_exchange_partition)
FAIL_POINT_REGISTER(region_exception_after_read_from_storage)
FAIL_POINT_REGISTER(pause_after_learner_read)

// Macros to set failpoint
#define FAIL_POINT_TRIGGER_EXCEPTION(fail_point) \
    fiu_do_on(fail_point, throw Exception("Fail point " #fail_point " is triggered.", ErrorCodes::FAIL_POINT_ERROR);)
#define FAIL_POINT_PAUSE(fail_point) \
    fiu_do_on(fail_point, FailPointHelper::wait(fail_point);)
// #define FAIL_POINT_TRIGGER_REGION_EXCEPTION(fail_point) fiu_do_on(fail_point, throw RegionException(); )


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

class FailPointHelper
{
public:
    static void enableFailPoint(const String & fail_point_name)
    {
        if (false) {}
        FAIL_POINT_ENABLE(fail_point_name, exception_between_alter_data_and_meta)
        FAIL_POINT_ENABLE(fail_point_name, exception_between_drop_meta_and_data)
        FAIL_POINT_ENABLE(fail_point_name, exception_drop_table_during_remove_meta)
        FAIL_POINT_ENABLE(fail_point_name, exception_between_rename_table_data_and_metadata)
        FAIL_POINT_ENABLE(fail_point_name, exception_between_create_database_meta_and_directory)
        FAIL_POINT_ENABLE(fail_point_name, exception_before_rename_table_old_meta_removed)
        FAIL_POINT_ENABLE(fail_point_name, exception_after_step_1_in_exchange_partition)
        FAIL_POINT_ENABLE(fail_point_name, exception_before_step_2_rename_in_exchange_partition)
        FAIL_POINT_ENABLE(fail_point_name, exception_after_step_2_in_exchange_partition)
        FAIL_POINT_ENABLE(fail_point_name, exception_before_step_3_rename_in_exchange_partition)
        FAIL_POINT_ENABLE(fail_point_name, exception_after_step_3_in_exchange_partition)
        FAIL_POINT_ENABLE(fail_point_name, region_exception_after_read_from_storage)
        FAIL_POINT_ENABLE_WITH_CHANNEL(fail_point_name, pause_after_learner_read)
        else throw Exception("Cannot find fail point " + fail_point_name, ErrorCodes::FAIL_POINT_ERROR);
    }

    static void disableFailPoint(const String & fail_point_name)
    {
        if (auto iter = fail_point_wait_channels.find(fail_point_name); iter != fail_point_wait_channels.end())
            fail_point_wait_channels.erase(iter);
        fiu_disable(fail_point_name.c_str());
    }

    static void wait(const String & fail_point_name)
    {
        if (auto iter = fail_point_wait_channels.find(fail_point_name); iter == fail_point_wait_channels.end())
            throw Exception("Can not find channel for fail point" + fail_point_name);
        else
        {
            auto ptr = iter->second;
            ptr->wait();
        }
    }

private:
    static std::unordered_map<String, std::shared_ptr<FailPointChannel>> fail_point_wait_channels;
};
} // namespace DB
