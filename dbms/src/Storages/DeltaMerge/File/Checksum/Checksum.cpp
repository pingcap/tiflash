//
// Created by schrodinger on 7/5/21.
//

#include "Checksum.h"

namespace DB::DM
{
template class B64Digest<Digest::None>;
template class B64Digest<Digest::CRC32>;
template class B64Digest<Digest::CRC64>;
template class B64Digest<Digest::City128>;
template class B64Digest<Digest::XXH3>;
} // namespace DB::DM