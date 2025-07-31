// Copyright 2025 PingCAP, Inc.
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

#include <Operators/CTE.h>

#include <memory>
#include <mutex>

namespace DB
{
// Data race is prevented by the lock in CTEManager
class CTEWithCounter
{
public:
    explicit CTEWithCounter(std::shared_ptr<CTE> cte_, Int32 expected_total_sink_num_, Int32 expected_total_source_num_)
        : cte(cte_)
        , expected_sink_num(expected_total_sink_num_)
        , expected_source_num(expected_total_source_num_)
    {}

    void sinkExit() { this->sink_exit_num++; }
    void sourceExit() { this->source_exit_num++; }

    Int32 getSinkExitNum() const { return this->sink_exit_num; }
    Int32 getSourceExitNum() const { return this->source_exit_num; }
    Int32 getTotalExitNum() const { return this->getSinkExitNum() + this->getSourceExitNum(); }

    Int32 getExpectedSinkNum() const { return this->expected_sink_num; }
    Int32 getExpectedTotalNum() const { return this->getExpectedSinkNum() + this->expected_source_num; }

    std::shared_ptr<CTE> getCTE() const { return this->cte; }

private:
    std::shared_ptr<CTE> cte;

    Int32 sink_exit_num = 0;
    Int32 source_exit_num = 0;
    Int32 expected_sink_num;
    Int32 expected_source_num;
};

class CTEManager
{
public:
    std::shared_ptr<CTE> getCTE(
        const String & query_id_and_cte_id,
        Int32 concurrency,
        Int32 expected_sink_num,
        Int32 expected_source_num);
    void releaseCTEBySource(const String & query_id_and_cte_id);
    void releaseCTEBySink(const tipb::SelectResponse & resp, const String & query_id_and_cte_id);
    void releaseCTE(const String & query_id_and_cte_id);

private:
    std::mutex mu;
    std::unordered_map<String, CTEWithCounter> ctes;
};
} // namespace DB
