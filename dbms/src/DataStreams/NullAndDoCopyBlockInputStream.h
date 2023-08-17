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
#include <DataStreams/copyData.h>


namespace DB
{

class IBlockOutputStream;
using BlockOutputStreamPtr = std::shared_ptr<IBlockOutputStream>;


/** An empty stream of blocks.
  * But at the first read attempt, copies the data from the passed `input` to the `output`.
  * This is necessary to execute the query INSERT SELECT - the query copies data, but returns nothing.
  * The query could be executed without wrapping it in an empty BlockInputStream,
  *  but the progress of query execution and the ability to cancel the query would not work.
  */
class NullAndDoCopyBlockInputStream : public IProfilingBlockInputStream
{
public:
    NullAndDoCopyBlockInputStream(const BlockInputStreamPtr & input_, BlockOutputStreamPtr output_)
        : input(input_)
        , output(output_)
    {
        children.push_back(input_);
    }

    String getName() const override { return "NullAndDoCopy"; }

    Block getHeader() const override { return {}; }

protected:
    Block readImpl() override
    {
        copyData(*input, *output);
        return Block();
    }

private:
    BlockInputStreamPtr input;
    BlockOutputStreamPtr output;
};

} // namespace DB
