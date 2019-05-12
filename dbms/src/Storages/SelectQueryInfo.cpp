#include <Storages/RegionQueryInfo.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

SelectQueryInfo::SelectQueryInfo(const SelectQueryInfo & query_info_)
    : query(query_info_.query),
      sets(query_info_.sets),
      mvcc_query_info(query_info_.mvcc_query_info != nullptr ? std::make_unique<MvccQueryInfo>(*query_info_.mvcc_query_info) : nullptr)
{}

SelectQueryInfo::SelectQueryInfo(SelectQueryInfo && query_info_)
    : query(query_info_.query), sets(query_info_.sets), mvcc_query_info(std::move(query_info_.mvcc_query_info))
{}

} // namespace DB
