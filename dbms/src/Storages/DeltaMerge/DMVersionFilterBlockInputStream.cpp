#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.h>

#include <Storages/DeltaMerge/DMVersionFilterBlockInputStream.ipp>

namespace DB::DM
{
template <int MODE>
void DMVersionFilterBlockInputStream<MODE>::readPrefix()
{
    forEachChild([](IBlockInputStream & child) {
        child.readPrefix();
        return false;
    });
}

template <int MODE>
void DMVersionFilterBlockInputStream<MODE>::readSuffix()
{
    forEachChild([](IBlockInputStream & child) {
        child.readSuffix();
        return false;
    });
}

template <int MODE>
Block DMVersionFilterBlockInputStream<MODE>::readGeneric(FilterPtr & res_filter, bool return_filter)
{
    return readImpl(res_filter, return_filter);
}
template class DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_MVCC>;
template class DMVersionFilterBlockInputStream<DM_VERSION_FILTER_MODE_COMPACT>;

} // namespace DB::DM
