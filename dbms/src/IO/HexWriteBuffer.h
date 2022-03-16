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

#include <IO/WriteBuffer.h>


/// Since HexWriteBuffer is often created in the inner loop, we'll make its buffer size small.
#define DBMS_HEX_WRITE_BUFFER_SIZE 32


namespace DB
{
/** Everything that is written into it, translates to HEX (in capital letters) and writes to another WriteBuffer.
  */
class HexWriteBuffer final : public WriteBuffer
{
protected:
    char buf[DBMS_HEX_WRITE_BUFFER_SIZE];
    WriteBuffer & out;

    void nextImpl() override;

public:
    HexWriteBuffer(WriteBuffer & out_)
        : WriteBuffer(buf, sizeof(buf))
        , out(out_)
    {}
    ~HexWriteBuffer() override;
};

} // namespace DB
