// Copyright 2022 PingCAP, Ltd.
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

// TODO support profile info.
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
};

class Spiller
{
public:
    virtual ~Spiller() = default;

    virtual OperatorStatus spill() { throw Exception("Unsupport"); }
};

class Source : public Spiller
{
public:
    virtual ~Source() = default;

    virtual OperatorStatus read(Block & block) = 0;

    virtual Block readHeader() = 0;

    virtual OperatorStatus await() { return OperatorStatus::PASS; }
};
using SourcePtr = std::unique_ptr<Source>;

class Transform : public Spiller
{
public:
    virtual ~Transform() = default;

    // call fetchBlock first, and then call transform.
    virtual OperatorStatus fetchBlock(Block &) { return OperatorStatus::NO_OUTPUT; }
    virtual OperatorStatus transform(Block & /*block*/) = 0;

    virtual void transformHeader(Block & header) { transform(header); }

    virtual OperatorStatus await() { return OperatorStatus::SKIP; }
};
using TransformPtr = std::unique_ptr<Transform>;

class Sink : public Spiller
{
public:
    virtual ~Sink() = default;

    // call prepare first, and then call write.
    virtual OperatorStatus prepare() { return OperatorStatus::PASS; }
    virtual OperatorStatus write(Block && /*block*/) = 0;

    virtual OperatorStatus await() { return OperatorStatus::PASS; }
};
using SinkPtr = std::unique_ptr<Sink>;
} // namespace DB
