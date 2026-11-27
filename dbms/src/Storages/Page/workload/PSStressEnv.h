// Copyright 2023 PingCAP, Inc.
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

#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/PageStorage.h>
#include <fmt/format.h>

#include <atomic>


namespace DB::PS::tests
{
using PSPtr = std::shared_ptr<DB::PageStorage>;

enum class StressEnvStat
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

    std::atomic<StressEnvStat> status = StressEnvStat::STATUS_LOOP;

public:
    static StressEnvStatus & getInstance()
    {
        static StressEnvStatus instance;
        return instance;
    }

    bool isRunning() const { return status == StressEnvStat::STATUS_LOOP; }

    // statCode >= 0 -> success
    // statCode < 0 -> failure
    Int32 statCode() const
    {
        return static_cast<Int32>(status.load());
    }

    void setStat(StressEnvStat status_) { status = status_; }
    StressEnvStat getStat() const { return status.load(); }
};

struct StressEnv
{
    size_t num_writers = 1;
    size_t num_readers = 4;
    bool init_pages = false;
    bool dropdata = false;
    size_t gc_interval_s = 30;
    size_t timeout_s = 0;
    size_t read_delay_ms = 0;
    size_t num_writer_slots = 1;
    size_t avg_page_size = 2 * 1024 * 1024;
    size_t status_interval = 5;
    size_t situation_mask = 0;
    bool verify = true;
    size_t running_ps_version = 3;

    std::vector<std::string> paths;
    std::vector<std::string> failpoints;

    String toDebugString() const
    {
        return fmt::format(
            "{{ "
            "num_writers: {}, num_readers: {}, init_pages: {}"
            ", dropdata: {}, timeout_s: {}, read_delay_ms: {}, num_writer_slots: {}"
            ", avg_page_size: {}, paths: [{}], failpoints: [{}]"
            ", gc_interval_s: {}"
            ", status_interval: {}, verify: {}"
            ", situation_mask: {}"
            ", running_pagestorage_version: {}"
            "}}",
            num_writers,
            num_readers,
            init_pages,
            dropdata,
            timeout_s,
            read_delay_ms,
            num_writer_slots,
            avg_page_size,
            fmt::join(paths.begin(), paths.end(), ","),
            fmt::join(failpoints.begin(), failpoints.end(), ","),
            gc_interval_s,
            status_interval,
            verify,
            situation_mask,
            running_ps_version //
        );
    }

    LoggerPtr logger;


    static StressEnv parse(int argc, char ** argv);

    void setup();

    static LoggerPtr buildLogger(bool enable_color);
};
} // namespace DB::PS::tests
