#include <IO/ChecksumBuffer.h>

namespace DB
{
using namespace DB::Digest;

template class FramedChecksumReadBuffer<None>;
template class FramedChecksumReadBuffer<CRC32>;
template class FramedChecksumReadBuffer<CRC64>;
template class FramedChecksumReadBuffer<City128>;
template class FramedChecksumReadBuffer<XXH3>;

template class FramedChecksumWriteBuffer<None>;
template class FramedChecksumWriteBuffer<CRC32>;
template class FramedChecksumWriteBuffer<CRC64>;
template class FramedChecksumWriteBuffer<City128>;
template class FramedChecksumWriteBuffer<XXH3>;

} // namespace DB
