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
class CTEManager
{
public:
    std::shared_ptr<CTE> getOrCreateCTE(
        const String & query_id_and_cte_id,
        Int32 concurrency,
        Int32 expected_sink_num,
        Int32 expected_source_num);
    void releaseCTEBySource(const String & query_id_and_cte_id);
    void releaseCTEBySink(const tipb::SelectResponse & resp, const String & query_id_and_cte_id);
    void releaseCTE(const String & query_id_and_cte_id);

    bool hasCTEForTest(const String & query_id_and_cte_id);

private:
    std::mutex mu;
    std::unordered_map<String, std::shared_ptr<CTE>> ctes;
};
} // namespace DB
