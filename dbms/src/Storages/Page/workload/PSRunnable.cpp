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

#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/formatReadable.h>
#include <IO/Buffer/ReadBufferFromMemory.h>
#include <IO/Encryption/MockKeyManager.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/Page/PageDefinesBase.h>
#include <Storages/Page/WriteBatchImpl.h>
#include <Storages/Page/workload/PSRunnable.h>
#include <Storages/Page/workload/PSStressEnv.h>
#include <TestUtils/MockDiskDelegator.h>
#include <common/logger_useful.h>
#include <fmt/format.h>

#include <mutex>
#include <random>
#include <thread>

namespace DB::PS::tests
{

void GlobalStat::commit(const RandomPageId & c)
{
    std::lock_guard lock(mtx_page_id);
    commit_ids.insert(c.page_id);
    for (const auto & id : c.page_id_to_remove)
    {
        commit_ids.erase(id);
        pending_remove_ids.erase(id);
    }
}

void PSRunnable::run()
try
{
    auto tracker = MemoryTracker::create();
    tracker->setDescription(nullptr);
    current_memory_tracker = tracker.get();
    // If runImpl() return false, means it need break itself
    while (StressEnvStatus::getInstance().isRunning() && runImpl())
    {
        /*Just for no warning*/
    }
    auto peak = current_memory_tracker->getPeak();
    current_memory_tracker = nullptr;
    LOG_INFO(logger, "{} exit with peak memory usage: {}", description(), formatReadableSizeWithBinarySuffix(peak));
}
catch (...)
{
    // stop the whole testing
    StressEnvStatus::getInstance().setStat(StressEnvStat::STATUS_EXCEPTION);
    DB::tryLogCurrentException(logger);
}

size_t PSRunnable::getBytesUsed() const
{
    return bytes_used;
}

size_t PSRunnable::getPagesUsed() const
{
    return pages_used;
}

///
/// Writer
///

DB::ReadBufferPtr PSWriter::getRandomData()
{
    if (memory == nullptr)
    {
        memory = std::unique_ptr<char[]>(new char[buffer_size_max + 1]);
        for (size_t i = 0; i < buffer_size_max + 1; i++)
            memory[i] = i % 0xFF;
    }

    std::uniform_int_distribution<> dist(buffer_size_min, buffer_size_max);
    size_t gen_size = dist(gen);
    return std::make_shared<DB::ReadBufferFromMemory>(memory.get(), gen_size);
}

void PSWriter::setBufferSizeRange(size_t min, size_t max)
{
    RUNTIME_CHECK(max >= min);
    buffer_size_min = min;
    buffer_size_max = max;

    LOG_INFO(logger, "writer_{} set min={} max={}", id(), min, max);
}

void PSWriter::write(const RandomPageId & r)
{
    auto buff_ptr = getRandomData();

    DB::WriteBatch wb{DB::TEST_NAMESPACE_ID};
    wb.putPage(r.page_id, 0, buff_ptr, buff_ptr->buffer().size());
    for (const auto id : r.page_id_to_remove)
        wb.delPage(id);
    ps->write(std::move(wb));

    if (write_delay)
    {
        std::this_thread::sleep_for(write_delay.value());
    }

    pages_used += 1;
    bytes_used += buff_ptr->buffer().size();

    // verbose logging for debug
    // LOG_TRACE(logger, "write done, id={} page_id={} remove={}", index, r.page_id, r.page_id_to_remove);

    global_stat->commit(r);
}

bool PSWriter::runImpl()
{
    write(genRandomPageId());
    return true;
}

RandomPageId PSWriter::genRandomPageId()
{
    // std::normal_distribution<> distribution{static_cast<double>(max_page_id) / 2, 150};
    std::uniform_int_distribution<> dist(0ULL, max_page_id - 1);
    return RandomPageId(static_cast<DB::PageIdU64>(std::round(dist(gen))));
}

DB::ReadBufferPtr PSCommonWriter::getRandomData()
{
    // Calculate the fixed memory size
    size_t single_buff_size = buffer_size_max;
    size_t memory_size = single_buff_size * batch_buffer_nums;

    if (memory == nullptr)
    {
        memory = std::unique_ptr<char[]>(new char[memory_size]);
        for (size_t i = 0; i < memory_size; i++)
            memory[i] = i % 0xFF;
    }

    std::uniform_int_distribution<> dist(buffer_size_min, buffer_size_max);
    size_t gen_size = dist(gen);
    return std::make_shared<DB::ReadBufferFromMemory>(memory.get(), gen_size);
}

bool PSCommonWriter::runImpl()
{
    const auto r = genRandomPageId();

    size_t page_write = 0;
    size_t bytes_write = 0;
    // FIXME: update one page_id by multiple data in one write batch?
    DB::WriteBatch wb{DB::TEST_NAMESPACE_ID};
    for (size_t i = 0; i < batch_buffer_nums; ++i)
    {
        auto buff_ptr = getRandomData();
        if (data_sizes.empty())
        {
            wb.putPage(r.page_id, 0, buff_ptr, buff_ptr->buffer().size());
        }
        else
        {
            // mock test for wide table that store many in-page-offsets
            wb.putPage(r.page_id, 0, buff_ptr, buff_ptr->buffer().size(), data_sizes);
        }
        page_write += 1;
        bytes_write += buff_ptr->buffer().size();
    }
    for (const auto & page_id : r.page_id_to_remove)
        wb.delPage(page_id);

    ps->write(std::move(wb));

    if (write_delay)
    {
        std::this_thread::sleep_for(write_delay.value());
    }

    pages_used += page_write;
    bytes_used += bytes_write;

    // verbose logging for debug
    // LOG_TRACE(logger, "write done, id={} page_id={} remove={}", index, r.page_id, r.page_id_to_remove);
    global_stat->commit(r);
    bool keep_running = (batch_buffer_limit == 0 || bytes_used < batch_buffer_limit);
    return keep_running;
}

void PSCommonWriter::setBatchBufferNums(size_t numbers)
{
    batch_buffer_nums = numbers;
}

void PSCommonWriter::setBatchBufferLimit(size_t size_limit)
{
    batch_buffer_limit = size_limit;
}

void PSCommonWriter::setBatchBufferPageRange(size_t max_page_id_)
{
    max_page_id = max_page_id_;
}

void PSCommonWriter::setFieldSize(const DB::PageFieldSizes & data_sizes_)
{
    data_sizes = data_sizes_;
}

///
/// Reader
///

DB::PageIdU64s PSReader::genRandomPageIds()
{
    DB::PageIdU64s page_ids;
    for (size_t i = 0; i < num_pages_read; ++i)
    {
        std::uniform_int_distribution<> dist(0, max_page_id);
        page_ids.emplace_back(static_cast<DB::PageIdU64>(dist(gen)));
    }
    return page_ids;
}

bool PSReader::runImpl()
{
    DB::PageIdU64s page_ids = genRandomPageIds();
    if (page_ids.empty())
        return true;

    auto page_map = ps->read(DB::TEST_NAMESPACE_ID, page_ids);
    for (const auto & page : page_map)
    {
        if (heavy_read_delay_ms > 0)
        {
            usleep(heavy_read_delay_ms * 1000);
        }
        ++pages_used;
        bytes_used += page.second.data.size();
    }
    return true;
}

void PSReader::setReadDelay(size_t delay_ms)
{
    heavy_read_delay_ms = delay_ms;
}

void PSReader::setReadPageRange(size_t max_page_id_)
{
    max_page_id = max_page_id_;
}

void PSReader::setReadPageNums(size_t page_read_once_)
{
    num_pages_read = page_read_once_;
}

///
/// WindowWriter
///

void PSWindowWriter::setNormalDistributionSigma(size_t sigma_)
{
    sigma = sigma_;
}

RandomPageId PSWindowWriter::genRandomPageId()
{
    std::lock_guard page_id_lock(global_stat->mtx_page_id);
    DB::PageIdU64Set ids_to_del;
    DB::PageIdU64 page_id = [this, &ids_to_del]() {
        if (global_stat->right_id_boundary < 4 * sigma)
        {
            return global_stat->right_id_boundary++;
        }

        // Generate a random number in the window, normal dist by μ=0 and σ=sigma
        std::normal_distribution distribution{0.0, static_cast<double>(sigma)};
        auto random = std::round(distribution(gen));
        // 100 - (100 - 68)/2 == 84% probability that update the existing page id
        if (random <= sigma)
        {
            // Move this "random" near the right boundary - σ, (mock a hot write in an id range)
            // we will update the data in this page_id
            DB::PageIdU64 page_id = std::abs(global_stat->right_id_boundary - sigma + random);
            return std::max(page_id, global_stat->left_id_boundary.load());
        }

        // Else it is about 16% probability that we create a new page.
        // Also we consider the pages with id less than (right boundary - 4σ) have no chance (less than 0.01%
        // by the definition of normal distribution) for being read later, remove the pages.
        DB::PageIdU64 left_boundary = 0;
        if (global_stat->right_id_boundary > 3 * sigma) // ensure the new left boundary is not negative
            left_boundary = global_stat->right_id_boundary - 3 * sigma;
        global_stat->left_id_boundary = left_boundary;

        // Remove the page id that is not likely update/read any more
        for (const auto & id : global_stat->commit_ids)
        {
            if (id >= left_boundary)
                break;
            ids_to_del.insert(id);
            global_stat->pending_remove_ids.insert(id);
        }

        auto page_id = global_stat->right_id_boundary++;
        if (page_id % 200 == 0)
            LOG_INFO(logger, "Update boundary to [{}, {})", left_boundary, global_stat->right_id_boundary);
        return page_id;
    }();
    return RandomPageId(page_id, ids_to_del);
}

///
/// WindowReader
///

void PSWindowReader::setNormalDistributionSigma(size_t sigma_)
{
    sigma = sigma_;
}

DB::PageIdU64s PSWindowReader::genRandomPageIds()
{
    const auto page_id_boundary_copy = global_stat->right_id_boundary.load();
    // Nothing to read
    if (page_id_boundary_copy < num_pages_read)
        return {};

    const size_t read_right_boundary = page_id_boundary_copy - num_pages_read;

    // Generate a random number in the window, normal dist by μ=0 and σ=sigma
    std::normal_distribution<> distribution{0.0, static_cast<double>(sigma)};
    double r = distribution(gen);
    // id > (right boundary+σ) is likely not written, turn the `r` into the left side of boundary
    // for reading
    if (r > sigma)
        r = -r;
    double rand_id = std::round(read_right_boundary - sigma + r); // the rand_id is double since it could be < 0.0
    // Limit by boundary
    rand_id = std::max(rand_id, global_stat->left_id_boundary.load());
    rand_id = std::min(rand_id, read_right_boundary);

    DB::PageIdU64s page_ids;
    std::lock_guard lock(global_stat->mtx_page_id);
    {
        for (size_t id = rand_id; id < num_pages_read + rand_id; ++id)
        {
            if (global_stat->commit_ids.find(id) != global_stat->commit_ids.end()
                && global_stat->pending_remove_ids.find(id) == global_stat->pending_remove_ids.end())
            {
                page_ids.emplace_back(id);
            }
        }
    }

    return page_ids;
}

bool PSSnapshotReader::runImpl()
{
    snapshots.emplace_back(ps->getSnapshot(""));
    usleep(snapshot_get_interval_ms * 1000);
    return true;
}

void PSSnapshotReader::setSnapshotGetIntervalMs(size_t snapshot_get_interval_ms_)
{
    snapshot_get_interval_ms = snapshot_get_interval_ms_;
}

bool PSIncreaseWriter::runImpl()
{
    return PSCommonWriter::runImpl() && begin_page_id < end_page_id;
}

void PSIncreaseWriter::setPageRange(size_t page_range)
{
    begin_page_id = index * page_range + 1;
    end_page_id = (index + 1) * page_range + 1;
}

RandomPageId PSIncreaseWriter::genRandomPageId()
{
    return RandomPageId(begin_page_id++);
}
} // namespace DB::PS::tests
