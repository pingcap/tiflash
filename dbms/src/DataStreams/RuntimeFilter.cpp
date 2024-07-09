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

#include <DataStreams/RuntimeFilter.h>
#include <Interpreters/Set.h>
#include <Storages/DeltaMerge/FilterParser/FilterParser.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int SET_SIZE_LIMIT_EXCEEDED;
}

std::string RuntimeFilter::getSourceColumnName() const
{
    return source_column_name;
}

tipb::RuntimeFilterType RuntimeFilter::getRFType() const
{
    return rf_type;
}

tipb::Expr RuntimeFilter::getSourceExpr() const
{
    return source_expr;
}

std::string RuntimeFilter::getFailedReason()
{
    std::lock_guard<std::mutex> lock(mtx);
    return failed_reason;
}

void RuntimeFilter::setSourceColumnName(const std::string & source_column_name_)
{
    source_column_name = source_column_name_;
}

void RuntimeFilter::setINValuesSet(const std::shared_ptr<Set> & in_values_set_)
{
    in_values_set = in_values_set_;
}

void RuntimeFilter::setTimezoneInfo(const TimezoneInfo & timezone_info_)
{
    timezone_info = timezone_info_;
}

void RuntimeFilter::build()
{
    if (!DM::FilterParser::isRSFilterSupportType(target_expr.field_type().tp()))
    {
        throw TiFlashException(
            Errors::Coprocessor::Unimplemented,
            "The rs operator doesn't support target expr type:{}, rf_id:{}",
            target_expr.field_type().DebugString(),
            id);
    }
    if (source_expr.tp() != tipb::ExprType::ColumnRef)
    {
        throw TiFlashException(
            Errors::Coprocessor::BadRequest,
            "The source expr {} of rf {} should be column ref",
            tipb::ExprType_Name(source_expr.tp()),
            id);
    }
}

// There is a possibility that the same rf is canceled multiple times,
// but rf does not have thread safety issues.
void RuntimeFilter::updateValues(const ColumnWithTypeAndName & values, const LoggerPtr & log)
{
    if (isFailed())
    {
        return;
    }
    Block data(std::initializer_list<ColumnWithTypeAndName>({values}));
    switch (rf_type)
    {
    case tipb::IN:
        try
        {
            in_values_set->insertFromBlock(data, false);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::SET_SIZE_LIMIT_EXCEEDED)
            {
                std::string tmp_err_msg = "The rf in values exceed the limit. " + e.message();
                updateStatus(RuntimeFilterStatus::FAILED, tmp_err_msg);
                LOG_WARNING(log, "cancel runtime filter id:{}, reason: {} ", id, tmp_err_msg);
            }
        }
        break;
    case tipb::MIN_MAX:
    case tipb::BLOOM_FILTER:
        // todo
        break;
    }
}

void RuntimeFilter::finalize(const LoggerPtr & log)
{
    if (!updateStatus(RuntimeFilterStatus::READY))
    {
        return;
    }
    std::string rf_values_info;
    switch (rf_type)
    {
    case tipb::IN:
        rf_values_info = fmt::format("number of IN values:{}", in_values_set->getTotalRowCount());
        break;
    case tipb::MIN_MAX:
    case tipb::BLOOM_FILTER:
        // TODO
        break;
    }
    LOG_INFO(log, "finalize runtime filter id:{}, rf values info:{}", id, rf_values_info);
}

void RuntimeFilter::cancel(const LoggerPtr & log, const std::string & reason)
{
    updateStatus(RuntimeFilterStatus::FAILED, reason);
    LOG_INFO(log, "cancel runtime filter id:{}, reason:{}", id, reason);
}

bool RuntimeFilter::isReady()
{
    return status == RuntimeFilterStatus::READY;
}

bool RuntimeFilter::isFailed()
{
    return status == RuntimeFilterStatus::FAILED;
}

bool RuntimeFilter::await(int64_t ms_remaining)
{
    if (isFailed())
    {
        return false;
    }
    if (!isReady())
    {
        if (ms_remaining <= 0)
        {
            return isReady();
        }
        std::unique_lock<std::mutex> lock(inner_mutex);
        inner_cv.wait_for(lock, std::chrono::milliseconds(ms_remaining), [this] { return isReady() || isFailed(); });
        return isReady();
    }
    return true;
}

bool RuntimeFilter::updateStatus(RuntimeFilterStatus status_, const std::string & reason)
{
    // check and update status
    {
        std::lock_guard<std::mutex> lock(mtx);
        switch (status_)
        {
        case RuntimeFilterStatus::NOT_READY:
            // The initial state of the runtime filter is not ready. It can only be updated as FAILED or READY.
            return false;
        case RuntimeFilterStatus::FAILED:
        {
            {
                if (status == RuntimeFilterStatus::READY)
                {
                    return false;
                }
                if (failed_reason.empty())
                {
                    failed_reason = reason;
                }
                break;
            }
        }
        case RuntimeFilterStatus::READY:
            // status cannot be repeatedly updated to ready
            if (status == RuntimeFilterStatus::READY || status == RuntimeFilterStatus::FAILED)
            {
                return false;
            }
            break;
        }
        status = status_;
    }
    inner_cv.notify_all();
    return true;
}

DM::RSOperatorPtr RuntimeFilter::parseToRSOperator(DM::ColumnDefines & columns_to_read) const
{
    switch (rf_type)
    {
    case tipb::IN:
        // Note that the elements are added from the block read (after timezone casted).
        // Take care of them when parsing to rough set filter.
        return DM::FilterParser::parseRFInExpr(
            rf_type,
            target_expr,
            columns_to_read,
            in_values_set->getUniqueSetElements(),
            timezone_info);
    case tipb::MIN_MAX:
    case tipb::BLOOM_FILTER:
        // TODO
    default:
        throw Exception("Unsupported rf type");
    }
}

} // namespace DB
