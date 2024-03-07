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

#include <IO/Buffer/WriteBufferFromFileBase.h>

namespace DB
{
WriteBufferFromFileBase::WriteBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment)
    : BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment)
{}

WriteBufferFromFileBase::~WriteBufferFromFileBase() = default;

off_t WriteBufferFromFileBase::seek(off_t off, int whence)
{
    return doSeek(off, whence);
}

void WriteBufferFromFileBase::truncate(off_t length)
{
    return doTruncate(length);
}

} // namespace DB
