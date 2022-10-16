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

#include <Common/nocopyable.h>
#include <gtest/gtest.h>

#include <memory>
#include <mutex>

namespace DB::tests
{

/**
 * Wrapping the underlying TestEventListener with a mutex.
 * This class can be used to make sure there is no interleaved outputs when
 * gtest is used concurrently with Poco logging.
 */
class SyncTestEventListener : public ::testing::EmptyTestEventListener
{
public:
    DISALLOW_COPY_AND_MOVE(SyncTestEventListener);

    SyncTestEventListener(const std::shared_ptr<std::mutex> mutex_, ::testing::TestEventListener * inner_)
        : mutex(mutex_)
        , inner(inner_)
    {
    }

    ~SyncTestEventListener() override = default;

    void OnTestProgramStart(const ::testing::UnitTest & unit_test) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnTestProgramStart(unit_test);
    }

    void OnTestIterationStart(const ::testing::UnitTest & unit_test,
                              int iteration) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnTestIterationStart(unit_test, iteration);
    }

    void OnEnvironmentsSetUpStart(const ::testing::UnitTest & unit_test) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnEnvironmentsSetUpStart(unit_test);
    }

    void OnEnvironmentsSetUpEnd(const ::testing::UnitTest & unit_test) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnEnvironmentsSetUpEnd(unit_test);
    }

    void OnTestCaseStart(const ::testing::TestCase & test_case) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnTestCaseStart(test_case);
    }

    void OnTestStart(const ::testing::TestInfo & test_info) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnTestStart(test_info);
    }

    void OnTestPartResult(const ::testing::TestPartResult & test_part_result) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnTestPartResult(test_part_result);
    }

    void OnTestEnd(const ::testing::TestInfo & test_info) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnTestEnd(test_info);
    }

    void OnTestCaseEnd(const ::testing::TestCase & test_case) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnTestCaseEnd(test_case);
    }

    void OnEnvironmentsTearDownStart(const ::testing::UnitTest & unit_test) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnEnvironmentsTearDownStart(unit_test);
    }

    void OnEnvironmentsTearDownEnd(const ::testing::UnitTest & unit_test) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnEnvironmentsTearDownEnd(unit_test);
    }

    void OnTestIterationEnd(const ::testing::UnitTest & unit_test,
                            int iteration) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnTestIterationEnd(unit_test, iteration);
    }

    void OnTestProgramEnd(const ::testing::UnitTest & unit_test) override
    {
        std::scoped_lock lock(*mutex);
        inner->OnTestProgramEnd(unit_test);
    }

private:
    std::shared_ptr<std::mutex> mutex;
    std::unique_ptr<::testing::TestEventListener> inner;
};

} // namespace DB::tests
