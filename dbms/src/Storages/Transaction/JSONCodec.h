#pragma once

#include <Core/Types.h>

namespace DB
{

void SkipJson(size_t & cursor, const String & raw_value);
String DecodeJsonAsBinary(size_t & cursor, const String & raw_value);
String DecodeJsonAsString(size_t & cursor, const String & raw_value);

} // namespace DB