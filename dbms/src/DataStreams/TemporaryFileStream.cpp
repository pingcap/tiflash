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

#include <Common/ClickHouseRevision.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/TemporaryFileStream.h>

namespace DB
{
TemporaryFileStream::TemporaryFileStream(const std::string & path, const FileProviderPtr & file_provider_)
    : file_provider{file_provider_}
    , file_in(file_provider, path, EncryptionPath(path, ""))
    , compressed_in(file_in)
    , block_in(std::make_shared<NativeBlockInputStream>(compressed_in, ClickHouseRevision::get()))
{}

TemporaryFileStream::~TemporaryFileStream()
{
    file_provider->deleteRegularFile(file_in.getFileName(), EncryptionPath(file_in.getFileName(), ""));
}
} // namespace DB
