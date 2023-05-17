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

#include <Common/Logger.h>
#include <Core/Block.h>

#include <memory>

namespace DB
{
/**
 * All interfaces of the operator may return the following state.
 * - finish status will only be returned by sink op, because only sink can tell if the pipeline has actually finished.
 * - cancel status, waiting status and io status can be returned in all method of operator.
 * - operator may return a different running status depending on the method.
*/
enum class OperatorStatus
{
    /// finish status
    FINISHED,
    /// cancel status
    CANCELLED,
    /// waiting status
    WAITING,
    /// io status
    IO,
    /// running status
    // means that TransformOp/SinkOp needs to input a block to do the calculation,
    NEED_INPUT,
    // means that SourceOp/TransformOp outputs a block as input to the subsequent operators.
    HAS_OUTPUT,
};

// TODO support operator profile info like `BlockStreamProfileInfo`.

class PipelineExecutorStatus;

class Operator
{
public:
    Operator(PipelineExecutorStatus & exec_status_, const String & req_id)
        : exec_status(exec_status_)
        , log(Logger::get(req_id))
    {}

    virtual ~Operator() = default;

    // running status may return are NEED_INPUT and HAS_OUTPUT here.
    OperatorStatus executeIO();
    virtual OperatorStatus executeIOImpl() { throw Exception("Unsupport"); }

    // running status may return are NEED_INPUT and HAS_OUTPUT here.
    OperatorStatus await();
    virtual OperatorStatus awaitImpl() { throw Exception("Unsupport"); }
    virtual bool isAwaitable() const { return false; }

    // These two methods are used to set state, log and etc, and should not perform calculation logic.
    virtual void operatePrefix() {}
    virtual void operateSuffix() {}

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
    PipelineExecutorStatus & exec_status;
    const LoggerPtr log;
    Block header;
};

// The running status returned by Source can only be `HAS_OUTPUT`.
class SourceOp : public Operator
{
public:
    SourceOp(PipelineExecutorStatus & exec_status_, const String & req_id)
        : Operator(exec_status_, req_id)
    {}
    // read will inplace the block when return status is HAS_OUTPUT;
    // Even after source has finished, source op still needs to return an empty block and HAS_OUTPUT,
    // because there are many operators that need an empty block as input, such as JoinProbe and WindowFunction.
    OperatorStatus read(Block & block);
    virtual OperatorStatus readImpl(Block & block) = 0;
};
using SourceOpPtr = std::unique_ptr<SourceOp>;
using SourceOps = std::vector<SourceOpPtr>;

class TransformOp : public Operator
{
public:
    TransformOp(PipelineExecutorStatus & exec_status_, const String & req_id)
        : Operator(exec_status_, req_id)
    {}
    // running status may return are NEED_INPUT and HAS_OUTPUT here.
    // tryOutput will inplace the block when return status is HAS_OUPUT; do nothing to the block when NEED_INPUT or others.
    OperatorStatus tryOutput(Block &);
    virtual OperatorStatus tryOutputImpl(Block &) { return OperatorStatus::NEED_INPUT; }
    // running status may return are NEED_INPUT and HAS_OUTPUT here.
    // transform will inplace the block and if the return status is HAS_OUTPUT, this block can be used as input to subsequent operators.
    // Even if an empty block is input, transform will still return HAS_OUTPUT,
    // because there are many operators that need an empty block as input, such as JoinProbe and WindowFunction.
    OperatorStatus transform(Block & block);
    virtual OperatorStatus transformImpl(Block & block) = 0;

    virtual void transformHeaderImpl(Block & header_) = 0;
    void transformHeader(Block & header_)
    {
        assert(header_);
        transformHeaderImpl(header_);
        setHeader(header_);
    }
};
using TransformOpPtr = std::unique_ptr<TransformOp>;
using TransformOps = std::vector<TransformOpPtr>;

// The running status returned by Sink can only be `NEED_INPUT`.
class SinkOp : public Operator
{
public:
    SinkOp(PipelineExecutorStatus & exec_status_, const String & req_id)
        : Operator(exec_status_, req_id)
    {}
    OperatorStatus prepare();
    virtual OperatorStatus prepareImpl() { return OperatorStatus::NEED_INPUT; }

    OperatorStatus write(Block && block);
    virtual OperatorStatus writeImpl(Block && block) = 0;
};
using SinkOpPtr = std::unique_ptr<SinkOp>;
} // namespace DB
