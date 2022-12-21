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

#include <DataStreams/LimitTransformAction.h>
#include <Operators/Operator.h>

namespace DB
{
class LimitTransform : public Transform
{
public:
    explicit LimitTransform(
        const GlobalLimitPtr & action_,
        const String & req_id)
        : action(action_)
        , log(Logger::get(req_id))
    {}

    OperatorStatus transform(Block & block) override;

    void transformHeader(Block & header) override;

private:
    GlobalLimitPtr action;
    const LoggerPtr log;
};
} // namespace DB
