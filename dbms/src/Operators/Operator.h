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
    NEED_MORE,
    MORE_OUTPUT,
    PASS,
    FINISHED,
};

class Source
{
public:
    virtual ~Source() = default;

    virtual Block read() = 0;

    virtual Block readHeader() = 0;
};
using SourcePtr = std::unique_ptr<Source>;

class Transform
{
public:
    virtual ~Transform() = default;

    virtual OperatorStatus transform(Block & /*block*/) = 0;

    virtual void transformHeader(Block & header)
    {
        transform(header);
    }

    virtual Block fetchBlock()
    {
        throw Exception("Unsupport");
    }
};
using TransformPtr = std::unique_ptr<Transform>;

class Sink
{
public:
    virtual ~Sink() = default;

    virtual OperatorStatus write(Block && /*block*/) = 0;
};
using SinkPtr = std::unique_ptr<Sink>;
} // namespace DB
