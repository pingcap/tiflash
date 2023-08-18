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

#include <memory>

namespace DB
{

/** Provides reading from a Buffer, taking exclusive ownership over it's lifetime,
  *    simplifies usage of ReadBufferFromFile (no need to manage buffer lifetime) etc.
  */
template <typename OwnType>
class OwningBlockInputStream : public IProfilingBlockInputStream
{
public:
    OwningBlockInputStream(const BlockInputStreamPtr & stream, std::unique_ptr<OwnType> own)
        : stream{stream}
        , own{std::move(own)}
    {
        children.push_back(stream);
    }

    Block getHeader() const override { return children.at(0)->getHeader(); }

private:
    Block readImpl() override { return stream->read(); }

    String getName() const override { return "Owning"; }

protected:
    BlockInputStreamPtr stream;
    std::unique_ptr<OwnType> own;
};

} // namespace DB
