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

#include <IO/Buffer/ReadBuffer.h>


namespace DB
{

namespace
{
template <typename CustomData>
class ReadBufferWrapper : public ReadBuffer
{
public:
    ReadBufferWrapper(ReadBuffer & in_, CustomData && custom_data_)
        : ReadBuffer(in_.buffer().begin(), in_.buffer().size(), in_.offset())
        , in(in_)
        , custom_data(std::move(custom_data_))
    {}

private:
    ReadBuffer & in;
    CustomData custom_data;

    bool nextImpl() override
    {
        in.position() = position();
        if (!in.next())
        {
            set(in.position(), 0);
            return false;
        }
        BufferBase::set(in.buffer().begin(), in.buffer().size(), in.offset());
        return true;
    }
};
} // namespace


std::unique_ptr<ReadBuffer> wrapReadBufferReference(ReadBuffer & ref)
{
    return std::make_unique<ReadBufferWrapper<nullptr_t>>(ref, nullptr);
}

std::unique_ptr<ReadBuffer> wrapReadBufferPointer(ReadBufferPtr ptr)
{
    return std::make_unique<ReadBufferWrapper<ReadBufferPtr>>(*ptr, ReadBufferPtr{ptr});
}

} // namespace DB
