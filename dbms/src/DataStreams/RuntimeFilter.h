// Copyright 2023 PingCAP, Inc.
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

#include <Columns/IColumn.h>
#include <Interpreters/Set.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/Filter/RSOperator_fwd.h>
#include <tipb/executor.pb.h>

namespace DB
{
enum class RuntimeFilterStatus
{
    NOT_READY,
    READY,
    FAILED
};

class RuntimeFilter
{
public:
    explicit RuntimeFilter(tipb::RuntimeFilter & rf_pb)
        : id(rf_pb.id())
        , rf_type(rf_pb.rf_type())
    {
        if (rf_pb.source_expr_list().size() != 1 || rf_pb.target_expr_list().size() != 1)
        {
            throw TiFlashException(
                Errors::Coprocessor::BadRequest,
                "for Runtime Filter, the size of source_expr_list and target_expr_list should both be 1 while "
                "source_expr_list size:{} target_expr_list size:{}",
                rf_pb.source_expr_list().size(),
                rf_pb.target_expr_list().size());
        }
        source_expr = rf_pb.source_expr_list().Get(0);
        target_expr = rf_pb.target_expr_list().Get(0);
    }

    std::string getSourceColumnName() const;

    tipb::RuntimeFilterType getRFType() const;

    tipb::Expr getSourceExpr() const;

    std::string getFailedReason();

    void setSourceColumnName(const std::string & source_column_name_);

    void setINValuesSet(const std::shared_ptr<Set> & in_values_set_);

    void setTimezoneInfo(const TimezoneInfo & timezone_info_);

    void build();

    void updateValues(const ColumnWithTypeAndName & values, const LoggerPtr & log);

    void finalize(const LoggerPtr & log);

    void cancel(const LoggerPtr & log, const std::string & reason);

    bool isReady();

    bool isFailed();

    bool await(int64_t ms_remaining);

    void setTargetAttr(const TiDB::ColumnInfos & scan_column_infos, const DM::ColumnDefines & table_column_defines);
    DM::RSOperatorPtr parseToRSOperator() const;

    const int id;

private:
    bool updateStatus(RuntimeFilterStatus status_, const std::string & reason = "");

    tipb::Expr source_expr;
    tipb::Expr target_expr;
    std::optional<DM::Attr> target_attr;
    const tipb::RuntimeFilterType rf_type;
    TimezoneInfo timezone_info;
    // thread safe
    std::atomic<RuntimeFilterStatus> status = RuntimeFilterStatus::NOT_READY;
    // used for failed_reason thread safe
    std::mutex mtx;
    std::string failed_reason;

    // after transform
    std::string source_column_name;
    // only used for In predicate
    // thread safe
    SetPtr in_values_set;
    // todo min max

    // used for await or signal
    std::mutex inner_mutex;
    std::condition_variable inner_cv;
};

} // namespace DB
