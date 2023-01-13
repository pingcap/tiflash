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
    OperatorStatus await();
    virtual OperatorStatus awaitImpl() { throw Exception("Unsupport"); }
    // running status may return are
    // - `NEED_INPUT` means that operator need data to spill.
    // - `HAS_OUTPUT` means that operator has restored data, and ready for ouput.
    OperatorStatus spill();
    virtual OperatorStatus spillImpl() { throw Exception("Unsupport"); }

    virtual String getName() const = 0;

    /** Get data structure of the operator in a form of "header" block (it is also called "sample block").
      * Header block contains column names, data types, columns of size 0. Constant columns must have corresponding values.
      */
    Block getHeader() const
    {
        assert(header);
        return header;
    }
    void setHeader(const Block & header_)
    {
        assert(header_ && !header);
        header = header_;
    }

protected:
    Block header;
};

// The running status returned by Source can only be `HAS_OUTPUT`.
class SourceOp : public Operator
{
public:
    // read will inplace the block when return status is HAS_OUTPUT;
    // Even after source has finished, source op still needs to return the empty block and HAS_OUTPUT,
    // because there are many operators that need the empty block as input, such as JoinProbe and WindowFunction.
    OperatorStatus read(Block & block);
    virtual OperatorStatus readImpl(Block & block) = 0;

    OperatorStatus awaitImpl() override { return OperatorStatus::HAS_OUTPUT; }
};
using SourceOpPtr = std::unique_ptr<SourceOp>;

class TransformOp : public Operator
{
public:
    // running status may return are NEED_INPUT and HAS_OUTPUT.
    // tryOutput will inplace the block when return status is HAS_OUPUT; do nothing to the block when NEED_INPUT or others.
    OperatorStatus tryOutput(Block &);
    virtual OperatorStatus tryOutputImpl(Block &) { return OperatorStatus::NEED_INPUT; }
    // running status may return are NEED_INPUT and PASS_THROUGH.
    // tryOutput will inplace the block when return status is PASS_THROUGH; do nothing to the block when NEED_INPUT or others.
    // Even if an empty block is input, transform will still return PASS_THROUGH,
    // because there are many operators that need the empty block as input, such as JoinProbe and WindowFunction.
    OperatorStatus transform(Block & block);
    virtual OperatorStatus transformImpl(Block & block) = 0;

    virtual void transformHeaderImpl(Block & header_) = 0;
    void transformHeader(Block & header_)
    {
        assert(header_);
        transformHeaderImpl(header_);
        setHeader(header_);
    }

    OperatorStatus awaitImpl() override { return OperatorStatus::NEED_INPUT; }
};
using TransformOpPtr = std::unique_ptr<TransformOp>;
using TransformOps = std::vector<TransformOpPtr>;

// The running status returned by Sink can only be `NEED_INPUT`.
class SinkOp : public Operator
{
public:
    virtual OperatorStatus prepare() { return OperatorStatus::NEED_INPUT; }

    OperatorStatus write(Block && block);
    virtual OperatorStatus writeImpl(Block && block) = 0;

    OperatorStatus awaitImpl() override { return OperatorStatus::NEED_INPUT; }
};
using SinkOpPtr = std::unique_ptr<SinkOp>;
} // namespace DB
