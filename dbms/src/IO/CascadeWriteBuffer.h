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
#include <IO/WriteBuffer.h>

#include <functional>


namespace DB
{
namespace ErrorCodes
{
extern const int CURRENT_WRITE_BUFFER_IS_EXHAUSTED;
}

/* The buffer is similar to ConcatReadBuffer, but writes data
 *
 * It has WriteBuffers sequence [prepared_sources, lazy_sources]
 * (lazy_sources contains not pointers themself, but their delayed constructors)
 *
 * Firtly, CascadeWriteBuffer redirects data to first buffer of the sequence
 * If current WriteBuffer cannot recieve data anymore, it throws special exception CURRENT_WRITE_BUFFER_IS_EXHAUSTED in nextImpl() body,
 *  CascadeWriteBuffer prepare next buffer and continuously redirects data to it.
 * If there are no buffers anymore CascadeWriteBuffer throws an exception.
 *
 * NOTE: If you use one of underlying WriteBuffers buffers outside, you need sync its position() with CascadeWriteBuffer's position().
 * The sync is performed into nextImpl(), getResultBuffers() and destructor.
 */
class CascadeWriteBuffer : public WriteBuffer
{
public:
    using WriteBufferPtrs = std::vector<WriteBufferPtr>;
    using WriteBufferConstructor = std::function<WriteBufferPtr(const WriteBufferPtr & prev_buf)>;
    using WriteBufferConstructors = std::vector<WriteBufferConstructor>;

    CascadeWriteBuffer(WriteBufferPtrs && prepared_sources_, WriteBufferConstructors && lazy_sources_ = {});

    void nextImpl() override;

    /// Should be called once
    void getResultBuffers(WriteBufferPtrs & res);

    const WriteBuffer * getCurrentBuffer() const { return curr_buffer; }

    ~CascadeWriteBuffer();

private:
    WriteBuffer * setNextBuffer();

    WriteBufferPtrs prepared_sources;
    WriteBufferConstructors lazy_sources;
    size_t first_lazy_source_num;
    size_t num_sources;

    WriteBuffer * curr_buffer;
    size_t curr_buffer_num;
};

} // namespace DB
