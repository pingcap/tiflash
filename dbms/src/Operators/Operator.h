// Copyright 2023 PingCAP, Ltd.
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

#include <Core/Block.h>

#include <memory>

namespace DB
{
enum class OperatorStatus
{
    /// finish status
    FINISHED,
    CANCELLED,
    /// spilling status
    SPILLING,
    /// waiting status
    WAITING,
    /// running status
    // means that TransformOp/SinkOp needs to input a block to do the calculation,
    // also used as an external state of `PipelineExec`.
    NEED_INPUT,
    // means that SourceOp/TransformOp can output blocks for used by subsequent operators.
    // also used as an external state of `PipelineExec`.
    HAS_OUTPUT,
    // means that TransformOp handle the input block and output it.
    PASS_THROUGH,
};

// TODO support operator profile info like `BlockStreamProfileInfo`.

class Operator
{
public:
    virtual ~Operator() = default;
    virtual OperatorStatus await() { throw Exception("Unsupport"); }
    virtual OperatorStatus spill() { throw Exception("Unsupport"); }
};

class SourceOp : public Operator
{
public:
    virtual OperatorStatus read(Block & block) = 0;

    virtual Block readHeader() = 0;

    OperatorStatus await() override { return OperatorStatus::HAS_OUTPUT; }
};
using SourceOpPtr = std::unique_ptr<SourceOp>;

class TransformOp : public Operator
{
public:
    // call tryOutput first, and then call transform.
    virtual OperatorStatus tryOutput(Block &) { return OperatorStatus::NEED_INPUT; }
    virtual OperatorStatus transform(Block & block) = 0;

    virtual void transformHeader(Block & header) { transform(header); }

    OperatorStatus await() override { return OperatorStatus::NEED_INPUT; }
};
using TransformOpPtr = std::unique_ptr<TransformOp>;
using TransformOps = std::vector<TransformOpPtr>;

class SinkOp : public Operator
{
public:
    // call prepare first, and then call write.
    virtual OperatorStatus prepare() { return OperatorStatus::NEED_INPUT; }
    virtual OperatorStatus write(Block && block) = 0;

    OperatorStatus await() override { return OperatorStatus::NEED_INPUT; }
};
using SinkOpPtr = std::unique_ptr<SinkOp>;
} // namespace DB
