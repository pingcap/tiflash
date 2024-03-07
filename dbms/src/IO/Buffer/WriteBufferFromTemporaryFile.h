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

#include <IO/Buffer/IReadableWriteBuffer.h>
#include <IO/Buffer/WriteBuffer.h>
#include <IO/Buffer/WriteBufferFromFile.h>
#include <Poco/TemporaryFile.h>


namespace DB
{
/// Rereadable WriteBuffer, could be used as disk buffer
/// Creates unique temporary in directory (and directory itself)
class WriteBufferFromTemporaryFile
    : public WriteBufferFromFile
    , public IReadableWriteBuffer
{
public:
    using Ptr = std::shared_ptr<WriteBufferFromTemporaryFile>;

    static Ptr create(const std::string & tmp_dir);

    ~WriteBufferFromTemporaryFile() override;

protected:
    WriteBufferFromTemporaryFile(std::unique_ptr<Poco::TemporaryFile> && tmp_file);

    std::shared_ptr<ReadBuffer> getReadBufferImpl() override;

protected:
    std::unique_ptr<Poco::TemporaryFile> tmp_file;

    friend class ReadBufferFromTemporaryWriteBuffer;
};

} // namespace DB
