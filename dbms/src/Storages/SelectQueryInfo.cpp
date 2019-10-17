#include <Storages/SelectQueryInfo.h>

#include <Flash/Coprocessor/DAGQueryInfo.h>
#include <Storages/RegionQueryInfo.h>

namespace DB
{

SelectQueryInfo::SelectQueryInfo() = default;

SelectQueryInfo::SelectQueryInfo(const SelectQueryInfo & query_info_)
    : query(query_info_.query),
      sets(query_info_.sets),
      mvcc_query_info(query_info_.mvcc_query_info != nullptr ? std::make_unique<MvccQueryInfo>(*query_info_.mvcc_query_info) : nullptr),
      dag_query(query_info_.dag_query != nullptr ? std::make_unique<DAGQueryInfo>(*query_info_.dag_query) : nullptr)
{}

SelectQueryInfo::SelectQueryInfo(SelectQueryInfo && query_info_)
    : query(query_info_.query), sets(query_info_.sets), mvcc_query_info(std::move(query_info_.mvcc_query_info)),
    dag_query(std::move(query_info_.dag_query))
{}

SelectQueryInfo::~SelectQueryInfo() = default;

} // namespace DB
