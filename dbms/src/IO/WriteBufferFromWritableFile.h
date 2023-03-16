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

#include <IO/WriteBufferFromFileDescriptor.h>

namespace DB
{
class WritableFile;
using WritableFilePtr = std::shared_ptr<WritableFile>;

class WriteBufferFromWritableFile : public WriteBufferFromFileDescriptor
{
protected:
    void nextImpl() override;

public:
    WriteBufferFromWritableFile(
        WritableFilePtr file_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~WriteBufferFromWritableFile() override;

    void sync() override;

    std::string getFileName() const override;

    int getFD() const override;

    void close() override;

private:
    off_t doSeek(off_t offset, int whence) override;

    void doTruncate(off_t length) override;

private:
    WritableFilePtr file;
};

} // namespace DB
