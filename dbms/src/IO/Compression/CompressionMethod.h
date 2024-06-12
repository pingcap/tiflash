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

namespace DB
{
// `CompressionMethod` is used to indicate which compression algorithm to use by users.
// It will be translated to `CompressionMethodByte` and stored with compressed data.
enum class CompressionMethod
{
    LZ4 = 1,
    LZ4HC = 2, /// The format is the same as for LZ4. The difference is only in compression.
    ZSTD = 3, /// Experimental algorithm: https://github.com/Cyan4973/zstd
    QPL = 4, /// The Intel Query Processing Library (QPL) is an open-source library to provide high-performance query processing operations
    NONE = 5, /// No compression
};

} // namespace DB
