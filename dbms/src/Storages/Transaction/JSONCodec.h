#pragma once

#include <Core/Field.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#pragma GCC diagnostic pop




namespace DB
{

using JsonVar = Poco::Dynamic::Var;

String DecodeJsonAsString(size_t &cursor, const String &raw_value);
String DecodeJsonAsBinary(size_t &cursor, const String & raw_value);

}