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

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>


namespace DB
{
/** Writes the data to another buffer, replacing the invalid UTF-8 sequences with the specified sequence.
    * If the valid UTF-8 is already written, it works faster.
    * Note: before using the resulting string, destroy this object.
    */
class WriteBufferValidUTF8 : public BufferWithOwnMemory<WriteBuffer>
{
private:
    WriteBuffer & output_buffer;
    bool group_replacements;
    /// The last recorded character was `replacement`.
    bool just_put_replacement = false;
    std::string replacement;

    void putReplacement();
    void putValid(char * data, size_t len);

    void nextImpl() override;
    void finish();

public:
    static const size_t DEFAULT_SIZE;

    WriteBufferValidUTF8(
        WriteBuffer & output_buffer,
        bool group_replacements = true,
        const char * replacement = "\xEF\xBF\xBD",
        size_t size = DEFAULT_SIZE);

    virtual ~WriteBufferValidUTF8() override { finish(); }
};

} // namespace DB
