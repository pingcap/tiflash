#include <Storages/Transaction/AtomicDecodedRow.h>
#include <Storages/Transaction/DecodedRow.h>

namespace DB
{

template <>
AtomicDecodedRow<false>::~AtomicDecodedRow()
{
    auto ptr = decoded.load();
    if (ptr)
    {
        auto decoded_ptr = reinterpret_cast<DecodedRow *>(ptr);
        delete decoded_ptr;
        decoded = nullptr;
    }
}

template <>
const DecodedRow * AtomicDecodedRow<false>::load() const
{
    return reinterpret_cast<DecodedRow *>(decoded.load());
}

template <>
void AtomicDecodedRow<false>::atomicUpdate(DB::DecodedRow *& data) const
{
    void * expected = nullptr;
    if (!decoded.compare_exchange_strong(expected, (void *)data))
        delete data;
    data = nullptr;
}

} // namespace DB
