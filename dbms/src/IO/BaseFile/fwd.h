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

#include <memory>

namespace DB
{
class ReadLimiter;
using ReadLimiterPtr = std::shared_ptr<ReadLimiter>;
class WriteLimiter;
using WriteLimiterPtr = std::shared_ptr<WriteLimiter>;

class PosixRandomAccessFile;
class PosixWritableFile;
class PosixWriteReadableFile;

class RandomAccessFile;
using RandomAccessFilePtr = std::shared_ptr<RandomAccessFile>;

class WritableFile;
using WritableFilePtr = std::shared_ptr<WritableFile>;

class WriteReadableFile;
using WriteReadableFilePtr = std::shared_ptr<WriteReadableFile>;

} // namespace DB