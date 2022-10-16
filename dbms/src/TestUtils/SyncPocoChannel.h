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

#include <Poco/Channel.h>
#include <Poco/Message.h>

/**
 * Wrapping the underlying Channel with a mutex.
 * This class can be used to make sure there is no interleaved outputs when
 * gtest is used concurrently with Poco logging.
 */
namespace DB::tests
{
class SyncPocoChannel : public ::Poco::Channel
{
public:
    DISALLOW_COPY_AND_MOVE(SyncPocoChannel);

    SyncPocoChannel(const std::shared_ptr<std::mutex> mutex_, Poco::AutoPtr<::Poco::Channel> inner_)
        : mutex(mutex_)
        , inner(inner_)
    {
    }

    ~SyncPocoChannel() override = default;

    void open() override
    {
        inner->open();
    }

    void close() override
    {
        inner->close();
    }

    void log(const ::Poco::Message & msg) override
    {
        std::scoped_lock lock(*mutex);
        inner->log(msg);
    };

private:
    std::shared_ptr<std::mutex> mutex;
    Poco::AutoPtr<::Poco::Channel> inner;
};
} // namespace DB::tests
