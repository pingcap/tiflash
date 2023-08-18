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

#include <Flash/Coprocessor/DAGBlockOutputStream.h>

namespace DB
{
DAGBlockOutputStream::DAGBlockOutputStream(Block && header_, std::unique_ptr<DAGResponseWriter> response_writer_)
    : header(std::move(header_))
    , response_writer(std::move(response_writer_))
{}

void DAGBlockOutputStream::writePrefix()
{
    response_writer->prepare(header);
}

void DAGBlockOutputStream::write(const Block & block)
{
    response_writer->write(block);
}

void DAGBlockOutputStream::writeSuffix()
{
    // todo error handle
    response_writer->flush();
}

} // namespace DB
