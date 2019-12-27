#include <Storages/Transaction/TiKVDecodedValue.h>
#include <Storages/Transaction/TiKVValueExtra.h>

namespace DB
{

template <>
ValueExtraInfo<false>::~ValueExtraInfo()
{
    auto ptr = decoded.load();
    if (ptr)
    {
        auto decoded_ptr = reinterpret_cast<DecodedRowBySchema *>(ptr);
        delete decoded_ptr;
        decoded = nullptr;
    }
}

template <>
const DecodedRowBySchema * ValueExtraInfo<false>::load() const
{
    return reinterpret_cast<DecodedRowBySchema *>(decoded.load());
}

template <>
void ValueExtraInfo<false>::atomicUpdate(DB::DecodedRowBySchema *& data) const
{
    void * expected = nullptr;
    if (!decoded.compare_exchange_strong(expected, (void *)data))
        delete data;
    data = nullptr;
}

} // namespace DB
