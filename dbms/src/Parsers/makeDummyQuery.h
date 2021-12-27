#pragma once

#include <Parsers/IAST.h>

namespace DB
{
/// Always return "select 1"
ASTPtr makeDummyQuery();

} // namespace DB

