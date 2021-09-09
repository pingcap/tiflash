#pragma once

#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <fmt/format.h>

#include <atomic>

namespace Poco {
    class Logger;
}

namespace DB
{

namespace FailPoints
{
extern const char random_slow_page_storage_remove_expired_snapshots[];
extern const char random_slow_page_storage_list_all_live_files[];
} // namespace FailPoints

} // namespace DB


using PSPtr = std::shared_ptr<DB::PageStorage>;

enum StressEnvStat
{
    STATUS_EXCEPTION = -1,
    STATUS_LOOP = 1,
    STATUS_INTERRUPT = 2,
    STATUS_TIMEOUT = 3,
};

class StressEnvStatus
{
private:
    StressEnvStatus() = default;
    ~StressEnvStatus() = default;

public:
    static StressEnvStatus & getInstance()
    {
        static StressEnvStatus instance;
        return instance;
    }

    std::atomic<StressEnvStat> status = STATUS_LOOP;
    bool stat() const;
    int isSuccess() const;

    void setStat(enum StressEnvStat status_);
};

struct StressEnv
{
    static Poco::Logger * logger;

    size_t num_writers = 1;
    size_t num_readers = 4;
    bool init_pages = false;
    bool clean_before_run = false;
    size_t timeout_s = 0;
    size_t read_delay_ms = 0;
    size_t num_writer_slots = 1;
    size_t avg_page_size_mb = 1;
    size_t rand_seed = 0x123987;
    size_t status_interval = 1;
    size_t situation_mask = 0;

    std::vector<std::string> paths;
    std::vector<std::string> failpoints;

    String toDebugString() const
    {
        return fmt::format(
            "{{ "
            "num_writers: {}, num_readers: {}, clean_before_run: {}"
            ", timeout_s: {}, read_delay_ms: {}, num_writer_slots: {}"
            ", avg_page_size_mb: {}, rand_seed: {:08x} paths: [{}] failpoints: [{}]"
            ", status_interval: {}, situation_mask : {}"
            " }}",
            num_writers,
            num_readers,
            clean_before_run,
            timeout_s,
            read_delay_ms,
            num_writer_slots,
            avg_page_size_mb,
            rand_seed,
            fmt::join(paths.begin(), paths.end(), ","),
            fmt::join(failpoints.begin(), failpoints.end(), ","),
            status_interval,
            situation_mask
            //
        );
    }


    static void initGlobalLogger();

    static StressEnv parse(int argc, char ** argv);

    void setup();
};
