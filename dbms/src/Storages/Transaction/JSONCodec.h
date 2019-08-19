#pragma once

#include <Core/Types.h>

namespace DB
{

String DecodeJsonAsString(size_t & cursor, const String & raw_value);
String DecodeJsonAsBinary(size_t & cursor, const String & raw_value);

} // namespace DB