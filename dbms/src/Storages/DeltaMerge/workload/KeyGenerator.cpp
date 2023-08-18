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

#include <Storages/DeltaMerge/workload/KeyGenerator.h>
#include <Storages/DeltaMerge/workload/Options.h>
#include <fmt/core.h>

#include <atomic>
#include <mutex>
#include <random>

namespace DB::DM::tests
{
class IncrementalKeyGenerator : public KeyGenerator
{
public:
    IncrementalKeyGenerator(uint64_t key_count_, uint64_t start_key_)
        : key_count(key_count_)
        , start_key(start_key_)
        , key(0)
    {}

    uint64_t get64() override { return key.fetch_add(1, std::memory_order_relaxed) % key_count + start_key; }

private:
    const uint64_t key_count;
    const uint64_t start_key;
    std::atomic<uint64_t> key;
};

class UniformDistributionKeyGenerator : public KeyGenerator
{
public:
    UniformDistributionKeyGenerator(uint64_t dist_a, uint64_t dist_b)
        : rand_gen(std::random_device()())
        , uniform_dist(dist_a, dist_b)
    {}
    explicit UniformDistributionKeyGenerator(uint64_t key_count)
        : rand_gen(std::random_device()())
        , uniform_dist(0, key_count)
    {}

    uint64_t get64() override
    {
        std::lock_guard lock(mtx);
        return uniform_dist(rand_gen);
    }

private:
    std::mutex mtx;
    std::mt19937_64 rand_gen;
    std::uniform_int_distribution<uint64_t> uniform_dist;
};

class NormalDistributionKeyGenerator : public KeyGenerator
{
public:
    NormalDistributionKeyGenerator(double mean, double stddev)
        : rand_gen(std::random_device()())
        , normal_dist(mean, stddev)
    {}
    explicit NormalDistributionKeyGenerator(uint64_t key_count)
        : rand_gen(std::random_device()())
        , normal_dist(key_count / 2.0, key_count / 20.0)
    {}

    uint64_t get64() override
    {
        std::lock_guard lock(mtx);
        return normal_dist(rand_gen);
    }

private:
    std::mutex mtx;
    std::mt19937_64 rand_gen;
    std::normal_distribution<> normal_dist;
};

std::unique_ptr<KeyGenerator> KeyGenerator::create(const WorkloadOptions & opts)
{
    const auto & dist = opts.write_key_distribution;
    const auto & testing_type = opts.testing_type;
    if (testing_type == "s3_bench")
    {
        return std::make_unique<UniformDistributionKeyGenerator>(1 * 1024 * 1024, 100 * 1024 * 1024);
    }
    else if (dist == "uniform")
    {
        return std::make_unique<UniformDistributionKeyGenerator>(opts.max_key_count);
    }
    else if (dist == "incremental")
    {
        return std::make_unique<IncrementalKeyGenerator>(opts.max_key_count, 0);
    }
    else if (dist == "normal")
    {
        return std::make_unique<NormalDistributionKeyGenerator>(opts.max_key_count);
    }
    else
    {
        throw std::invalid_argument(fmt::format("KeyGenerator::create '{}' not support.", dist));
    }
}

} // namespace DB::DM::tests
