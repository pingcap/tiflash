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

#include <Storages/Page/workload/PSWorkload.h>

namespace DB::PS::tests
{
class ThousandsOfOffset
    : public StressWorkload
    , public StressWorkloadFunc<ThousandsOfOffset>
{
public:
    explicit ThousandsOfOffset(const StressEnv & options_)
        : StressWorkload(options_)
    {}

    static String name() { return "ThousandsOfOffset"; }

    static UInt64 mask() { return 1 << 5; }

private:
    String desc() override
    {
        return fmt::format(
            "Some of options will be ignored."
            "`paths` will only used first one. which is {}. Data will store in {}"
            "Please cleanup folder after this test."
            "The current workload will run 4 cases"
            "(case 1: single 1M buffer with field."
            "case 2: 20*1MB buffers with field."
            "case 3: single 100kb buffer with field."
            "case 4: 20*100kb buffer with field.)"
            "and elapse near 120 seconds",
            options.paths[0],
            options.paths[0] + "/" + name());
    }

    static DB::PageFieldSizes divideFields(size_t amount, size_t nums)
    {
        DB::PageFieldSizes field_sizes;
        size_t rest_amount = amount;
        size_t rest_nums = nums;
        for (size_t i = 0; i < nums - 1; i++)
        {
            size_t split = (random() % (rest_amount / rest_nums * 2) - 1) + 1;
            rest_amount -= split;
            rest_nums--;
            field_sizes.emplace_back(split);
        }

        field_sizes.emplace_back(rest_amount);
        return field_sizes;
    }

    void run() override
    {
        pool.addCapacity(1 + options.num_writers + options.num_readers);
        DB::PageStorageConfig config;
        initPageStorage(config, name());

        metrics_dumper = std::make_shared<PSMetricsDumper>(1, options.logger);
        metrics_dumper->start();

        {
            stress_time = std::make_shared<StressTimeout>(30, options.logger);
            stress_time->start();

            stop_watch.start();

            auto buffer_size = 1 * DB::MB;
            auto field_size = divideFields(buffer_size, 1000);

            startWriter<PSWindowWriter>(
                options.num_writers,
                [field_size, buffer_size](std::shared_ptr<PSWindowWriter> writer) -> void {
                    writer->setFieldSize(field_size);
                    writer->setBatchBufferNums(1);
                    writer->setBufferSizeRange(buffer_size, buffer_size);
                    writer->setNormalDistributionSigma(13);
                });

            pool.joinAll();
            stop_watch.stop();
            onDumpResult();
        }

        {
            stress_time = std::make_shared<StressTimeout>(30, options.logger);
            stress_time->start();

            stop_watch.start();

            auto buffer_size = 1 * DB::MB;
            auto field_size = divideFields(buffer_size, 1000);

            startWriter<PSWindowWriter>(
                options.num_writers,
                [field_size, buffer_size](std::shared_ptr<PSWindowWriter> writer) -> void {
                    writer->setFieldSize(field_size);
                    writer->setBatchBufferNums(20);
                    writer->setBufferSizeRange(buffer_size, buffer_size);
                    writer->setNormalDistributionSigma(13);
                });

            pool.joinAll();
            stop_watch.stop();
            onDumpResult();
        }

        {
            stress_time = std::make_shared<StressTimeout>(30, options.logger);
            stress_time->start();

            stop_watch.start();

            auto buffer_size = 100 * 1024;
            auto field_size = divideFields(buffer_size, 1000);

            startWriter<PSWindowWriter>(
                options.num_writers,
                [field_size, buffer_size](std::shared_ptr<PSWindowWriter> writer) -> void {
                    writer->setFieldSize(field_size);
                    writer->setBatchBufferNums(1);
                    writer->setBufferSizeRange(buffer_size, buffer_size);
                    writer->setNormalDistributionSigma(13);
                });

            pool.joinAll();
            stop_watch.stop();
            onDumpResult();
        }

        {
            stress_time = std::make_shared<StressTimeout>(30, options.logger);
            stress_time->start();

            stop_watch.start();

            auto buffer_size = 100 * 1024;
            auto field_size = divideFields(buffer_size, 1000);

            startWriter<PSWindowWriter>(
                options.num_writers,
                [field_size, buffer_size](std::shared_ptr<PSWindowWriter> writer) -> void {
                    writer->setFieldSize(field_size);
                    writer->setBatchBufferNums(20);
                    writer->setBufferSizeRange(buffer_size, buffer_size);
                    writer->setNormalDistributionSigma(13);
                });

            pool.joinAll();
            stop_watch.stop();
        }
    }

    bool verify() override { return true; }
};
} // namespace DB::PS::tests
