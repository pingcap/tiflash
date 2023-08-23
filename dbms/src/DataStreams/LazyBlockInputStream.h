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

#include <DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Initialize another source on the first `read` call, and then use it.
  * This is needed, for example, to read from a table that will be populated
  *  after creation of LazyBlockInputStream object, but before the first `read` call.
  */
class LazyBlockInputStream : public IProfilingBlockInputStream
{
public:
    using Generator = std::function<BlockInputStreamPtr()>;

    LazyBlockInputStream(const Block & header_, Generator generator_)
        : header(header_)
        , generator(std::move(generator_))
    {}

    LazyBlockInputStream(const char * name_, const Block & header_, Generator generator_)
        : name(name_)
        , header(header_)
        , generator(std::move(generator_))
    {}

    String getName() const override { return name; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        if (!input)
        {
            input = generator();

            if (!input)
                return Block();

            auto * p_input = dynamic_cast<IProfilingBlockInputStream *>(input.get());

            if (p_input)
            {
                /// They could have been set before, but were not passed into the `input`.
                if (progress_callback)
                    p_input->setProgressCallback(progress_callback);
                if (process_list_elem)
                    p_input->setProcessListElement(process_list_elem);
            }

            input->readPrefix();

            {
                addChild(input);

                if (isCancelled() && p_input)
                    p_input->cancel(is_killed);
            }
        }

        return input->read();
    }

private:
    const char * name = "Lazy";
    Block header;
    Generator generator;

    BlockInputStreamPtr input;
};

} // namespace DB
