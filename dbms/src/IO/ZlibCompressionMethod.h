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

namespace DB
{
enum class ZlibCompressionMethod
{
    /// DEFLATE compression with gzip header and CRC32 checksum.
    /// This option corresponds to files produced by gzip(1) or HTTP Content-Encoding: gzip.
    Gzip,
    /// DEFLATE compression with zlib header and Adler32 checksum.
    /// This option corresponds to HTTP Content-Encoding: deflate.
    Zlib,
};

} // namespace DB
