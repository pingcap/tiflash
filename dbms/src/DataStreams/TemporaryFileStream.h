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

#include <DataStreams/IBlockInputStream.h>
#include <Encryption/FileProvider.h>
#include <Encryption/ReadBufferFromFileProvider.h>
#include <IO/CompressedReadBuffer.h>

#include <memory>
#include <vector>

namespace DB
{
/// To read the data that was flushed into the temporary data file.
struct TemporaryFileStream
{
    FileProviderPtr file_provider;
    ReadBufferFromFileProvider file_in;
    CompressedReadBuffer<> compressed_in;
    BlockInputStreamPtr block_in;

    TemporaryFileStream(const std::string & path, const FileProviderPtr & file_provider_);
    ~TemporaryFileStream();
};

using TemporaryFileStreams = std::vector<std::unique_ptr<TemporaryFileStream>>;
} // namespace DB
