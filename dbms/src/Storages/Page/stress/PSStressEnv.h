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

#include <Storages/Page/PageDefines.h>
#include <Storages/Page/PageStorage.h>
#include <fmt/format.h>

#include <atomic>

namespace Poco
{
class Logger;
}

using PSPtr = std::shared_ptr<DB::PageStorage>;

enum StressEnvStat
{
    // Below status are defined as fail
    STATUS_EXCEPTION = -1,
    STATUS_INTERRUPT = -2,
    // Below status are defined as success
    STATUS_LOOP = 1,
    STATUS_TIMEOUT = 2,
};

class StressEnvStatus
{
private:
    StressEnvStatus() = default;
    ~StressEnvStatus() = default;

    std::atomic<StressEnvStat> status = STATUS_LOOP;

public:
    static StressEnvStatus & getInstance()
    {
        static StressEnvStatus instance;
        return instance;
    }

    bool isRunning() const
    {
        return status == STATUS_LOOP;
    }
    int isSuccess() const
    {
        auto code = status.load();
        return code > 0 ? 0 : static_cast<int>(code);
    }

    void setStat(enum StressEnvStat status_)
    {
        status = status_;
    }
};

struct StressEnv
{
    static Poco::Logger * logger;

    size_t num_writers = 1;
    size_t num_readers = 4;
    bool init_pages = false;
    bool just_init_pages = false;
    bool clean_before_run = false;
    size_t timeout_s = 0;
    size_t read_delay_ms = 0;
    size_t num_writer_slots = 1;
    size_t avg_page_size_mb = 1;
    size_t status_interval = 1;
    size_t situation_mask = 0;
    bool verify = true;
    size_t running_ps_version = 3;

    std::vector<std::string> paths;
    std::vector<std::string> failpoints;

    String toDebugString() const
    {
        return fmt::format(
            "{{ "
            "num_writers: {}, num_readers: {}, init_pages: {}, just_init_pages: {}"
            ", clean_before_run: {}, timeout_s: {}, read_delay_ms: {}, num_writer_slots: {}"
            ", avg_page_size_mb: {}, paths: [{}], failpoints: [{}]"
            ", status_interval: {}, situation_mask: {}, verify: {}"
            ", running_pagestorage_version : {}."
            "}}",
            num_writers,
            num_readers,
            init_pages,
            just_init_pages,
            clean_before_run,
            timeout_s,
            read_delay_ms,
            num_writer_slots,
            avg_page_size_mb,
            fmt::join(paths.begin(), paths.end(), ","),
            fmt::join(failpoints.begin(), failpoints.end(), ","),
            status_interval,
            situation_mask,
            verify,
            running_ps_version
            //
        );
    }

    static void initGlobalLogger();

    static StressEnv parse(int argc, char ** argv);

    void setup();
};
