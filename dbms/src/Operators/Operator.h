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
    // spilling status
    SPILLING,
    // waiting status
    WAITING,
    SKIP,
    // running status
    PASS,
    NO_OUTPUT,
    MORE_INPUT,
    // finish status
    FINISHED,
    CANCELLED,
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

    OperatorStatus await() override { return OperatorStatus::PASS; }
};
using SourceOpPtr = std::unique_ptr<SourceOp>;

class TransformOp : public Operator
{
public:
    // call fetchBlock first, and then call transform.
    virtual OperatorStatus fetchBlock(Block &) { return OperatorStatus::NO_OUTPUT; }
    virtual OperatorStatus transform(Block & block) = 0;

    virtual void transformHeader(Block & header) { transform(header); }

    OperatorStatus await() override { return OperatorStatus::SKIP; }
};
using TransformOpPtr = std::unique_ptr<TransformOp>;
using TransformOps = std::vector<TransformOpPtr>;

class SinkOp : public Operator
{
public:
    // call prepare first, and then call write.
    virtual OperatorStatus prepare() { return OperatorStatus::PASS; }
    virtual OperatorStatus write(Block && block) = 0;

    OperatorStatus await() override { return OperatorStatus::PASS; }
};
using SinkOpPtr = std::unique_ptr<SinkOp>;
} // namespace DB
