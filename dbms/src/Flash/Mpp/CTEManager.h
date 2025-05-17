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
struct CTEWithCounter
{
    CTEWithCounter(std::shared_ptr<CTE> cte_, Int32 counter_)
        : cte(cte_)
        , counter(counter_)
    {}
    std::shared_ptr<CTE> cte;
    Int32 counter;
};

// TODO Test this class with UT
class CTEManager
{
public:
    std::shared_ptr<CTE> getCTE(const String & query_id_and_cte_id, const String & partition_id);
    void releaseCTE(const String & query_id_and_cte_id, const String & partition_id);
    void setRespAndNotifyEOF(const tipb::SelectResponse & resp, const String & query_id_and_cte_id);
    void notifyEOF(const String & query_id_and_cte_id);

private:
    template <bool set_resp>
    void executeOnManyCTEs(const tipb::SelectResponse & resp, const String & query_id_and_cte_id);

    std::mutex mu;
    std::map<String, std::map<String, CTEWithCounter>> ctes;
};
} // namespace DB
