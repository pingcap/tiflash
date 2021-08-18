#ifdef TIFLASH_ENABLE_AVX_SUPPORT
#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.ipp>
namespace DB::DM
{
template <>
Block DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_MVCC>::readAVX2(FilterPtr & res_filter, bool return_filter)
{
    return readImpl(res_filter, return_filter);
}
template <>
Block DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>::readAVX2(FilterPtr & res_filter, bool return_filter)
{
    return readImpl(res_filter, return_filter);
}
} // namespace DB::DM
#endif