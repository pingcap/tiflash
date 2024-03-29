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

#include <IO/Buffer/BufferWithOwnMemory.h>
#include <IO/Buffer/ReadBuffer.h>
#include <fcntl.h>

#include <ctime>
#include <functional>
#include <string>

#ifdef __APPLE__
#include <common/apple_rt.h>
#endif

namespace DB
{
class ReadBufferFromFileBase : public BufferWithOwnMemory<ReadBuffer>
{
public:
    ReadBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment);
    ReadBufferFromFileBase(ReadBufferFromFileBase &&) = default;
    ~ReadBufferFromFileBase() override;
    off_t seek(off_t off, int whence = SEEK_SET);
    virtual off_t getPositionInFile() = 0;
    virtual std::string getFileName() const = 0;
    virtual int getFD() const = 0;

    /// It is possible to get information about the time of each reading.
    struct ProfileInfo
    {
        size_t bytes_requested;
        size_t bytes_read;
        size_t nanoseconds;
    };

    using ProfileCallback = std::function<void(ProfileInfo)>;

    /// CLOCK_MONOTONIC_COARSE is more than enough to track long reads - for example, hanging for a second.
    void setProfileCallback(const ProfileCallback & profile_callback_, clockid_t clock_type_ = CLOCK_MONOTONIC_COARSE)
    {
        profile_callback = profile_callback_;
        clock_type = clock_type_;
    }

protected:
    ProfileCallback profile_callback;
    clockid_t clock_type{};

    virtual off_t doSeek(off_t off, int whence) = 0;
};

} // namespace DB
