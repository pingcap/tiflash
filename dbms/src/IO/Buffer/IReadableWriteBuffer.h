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
#include <IO/Buffer/ReadBuffer.h>

#include <memory>

namespace DB
{
struct IReadableWriteBuffer
{
    /// At the first time returns getReadBufferImpl(). Next calls return nullptr.
    inline std::shared_ptr<ReadBuffer> tryGetReadBuffer()
    {
        if (!can_reread)
            return nullptr;

        can_reread = false;
        return getReadBufferImpl();
    }

    virtual ~IReadableWriteBuffer() = default;

protected:
    /// Creates read buffer from current write buffer.
    /// Returned buffer points to the first byte of original buffer.
    /// Original stream becomes invalid.
    virtual std::shared_ptr<ReadBuffer> getReadBufferImpl() = 0;

    bool can_reread = true;
};

} // namespace DB
