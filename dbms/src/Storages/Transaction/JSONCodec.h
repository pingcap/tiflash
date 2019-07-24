#pragma once

#include <Core/Field.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Array.h>
#pragma GCC diagnostic pop




namespace DB
{

using JsonArrayPtr = Poco::JSON::Array::Ptr;
using JsonObjectPtr = Poco::JSON::Object::Ptr;
using JsonVar = Poco::Dynamic::Var;


JsonArrayPtr decodeJSONArray(size_t & cursor, const String & raw_value);

String DecodeJson(size_t &cursor, const String &raw_value);

}