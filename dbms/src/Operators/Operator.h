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
/**
 * All interfaces of the operator may return the following state.
 * - finish status, spilling status and waiting status can be returned in all interfaces of operator.
 * - operator may return a different running status depending on the interface.
*/
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
    // means that SourceOp/TransformOp output a block for used by subsequent operators.
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
    // running status may return are
    // - `NEED_INPUT` means that the data that operator is waiting for has been prepared.
    virtual OperatorStatus await() { throw Exception("Unsupport"); }
    // running status may return are
    // - `NEED_INPUT` means that operator need data to spill.
    // - `HAS_OUTPUT` means that operator has restored data, and ready for ouput.
    virtual OperatorStatus spill() { throw Exception("Unsupport"); }
};

// The running status returned by Sink can only be `HAS_OUTPUT`.
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
    // may return NEED_INPUT and HAS_OUTPUT
    virtual OperatorStatus tryOutput(Block &) { return OperatorStatus::NEED_INPUT; }
    // may return NEED_INPUT and PASS_THROUGH
    virtual OperatorStatus transform(Block & block) = 0;

    virtual void transformHeader(Block & header) { transform(header); }

    OperatorStatus await() override { return OperatorStatus::NEED_INPUT; }
};
using TransformOpPtr = std::unique_ptr<TransformOp>;
using TransformOps = std::vector<TransformOpPtr>;

// The running status returned by Sink can only be `NEED_INPUT`.
class SinkOp : public Operator
{
public:
    virtual OperatorStatus prepare() { return OperatorStatus::NEED_INPUT; }
    virtual OperatorStatus write(Block && block) = 0;

    OperatorStatus await() override { return OperatorStatus::NEED_INPUT; }
};
using SinkOpPtr = std::unique_ptr<SinkOp>;
} // namespace DB
