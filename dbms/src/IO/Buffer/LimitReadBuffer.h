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

#include <cstddef>


namespace DB
{
/** Allows to read from another ReadBuffer no more than the specified number of bytes.
  */
class LimitReadBuffer : public ReadBuffer
{
private:
    ReadBuffer & in;
    size_t limit;

    bool nextImpl() override;

public:
    LimitReadBuffer(ReadBuffer & in_, size_t limit_);
    ~LimitReadBuffer() override;
};

} // namespace DB
