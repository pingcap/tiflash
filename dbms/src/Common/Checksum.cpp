#include <Common/Checksum.h>

namespace DB
{
template class UnifiedDigest<Digest::None>;
template class UnifiedDigest<Digest::CRC32>;
template class UnifiedDigest<Digest::CRC64>;
template class UnifiedDigest<Digest::City128>;
template class UnifiedDigest<Digest::XXH3>;
} // namespace DB