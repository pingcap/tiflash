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

#include <Common/Logger.h>
#include <Flash/Coprocessor/DAGPipeline.h>
#include <Interpreters/ExpressionActions.h>

namespace DB
{
class DAGContext;

void restoreConcurrency(
    DAGPipeline & pipeline,
    size_t concurrency,
    const LoggerPtr & log);

BlockInputStreamPtr combinedNonJoinedDataStream(
    DAGPipeline & pipeline,
    size_t max_threads,
    const LoggerPtr & log,
    bool ignore_block = false);

void executeUnion(
    DAGPipeline & pipeline,
    size_t max_streams,
    const LoggerPtr & log,
    bool ignore_block = false);

void executeExpression(
    DAGPipeline & pipeline,
    const ExpressionActionsPtr & expressionActionsPtr,
    const LoggerPtr & log);

void updateFinalConcurrency(
    DAGContext & dag_context,
    size_t cur_streams_size,
    size_t max_streams);
} // namespace DB
