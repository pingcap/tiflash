//
// Created by schrodinger on 7/5/21.
//

#include "Checksum.h"

namespace DB::DM
{
template class UnifiedDigest<Digest::None>;
template class UnifiedDigest<Digest::CRC32>;
template class UnifiedDigest<Digest::CRC64>;
template class UnifiedDigest<Digest::City128>;
template class UnifiedDigest<Digest::XXH3>;
} // namespace DB::DM