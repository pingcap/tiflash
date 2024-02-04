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

#include <IO/Checksum/ChecksumBuffer.h>

namespace DB
{
using namespace DB::Digest;

template class FramedChecksumReadBuffer<None>;
template class FramedChecksumReadBuffer<CRC32>;
template class FramedChecksumReadBuffer<CRC64>;
template class FramedChecksumReadBuffer<City128>;
template class FramedChecksumReadBuffer<XXH3>;

template class FramedChecksumWriteBuffer<None>;
template class FramedChecksumWriteBuffer<CRC32>;
template class FramedChecksumWriteBuffer<CRC64>;
template class FramedChecksumWriteBuffer<City128>;
template class FramedChecksumWriteBuffer<XXH3>;

} // namespace DB
