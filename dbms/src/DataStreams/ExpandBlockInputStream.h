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
#include <Flash/Planner/Plans/PhysicalExpand2.h>
#include <Interpreters/Expand2.h>


namespace DB
{

/** Executes N leveled projections over the block, output one of N block each time.
  * The projection expression consists of column identifiers from the block, literal(null/num).
  * For example: source column:  a,   b,    c
  *                  1 th proj:  a,   b,   null
  *                  2 th proj:  a,  null, null
  *                 maybe more:  ...
  *
  * The N leveled-projections processes each block and output N blocks respectively.
  *
  * Here we create a input stream for expand because we wanna cache source block inside and
  * control each output replica block according the level-projection one by one. (take control in readImpl)
  */
class ExpandBlockInputStream : public IProfilingBlockInputStream
{
private:
    static constexpr auto NAME = "Expand2";

public:
    ExpandBlockInputStream(
        const BlockInputStreamPtr & input,
        const Expand2Ptr & expand2_,
        const Block & header_,
        const String & req_id);

    String getName() const override { return NAME; }
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    Expand2Ptr expand2;
    // for header change
    Block header;
    // the expanding state is control by local variable block_cache and i_th_project in ExpandBlockInputStream.
    Block block_cache;
    // the i_th projection pointer.
    size_t i_th_project;
    const LoggerPtr log;
};

} // namespace DB
