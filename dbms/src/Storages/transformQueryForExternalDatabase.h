// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Core/Types.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

class IAST;
class Context;

/** For given ClickHouse query,
  * creates another query in a form of
  *
  * SELECT columns... FROM db.table WHERE ...
  *
  * where 'columns' are all required columns to read from "left" table of original query,
  * and WHERE contains subset of (AND-ed) conditions from original query,
  * that contain only compatible expressions.
  *
  * Compatible expressions are comparisons of identifiers, constants, and logical operations on them.
  *
  * NOTE There are concerns with proper quoting of identifiers for remote database.
  * Some databases use `quotes` and other use "quotes".
  */
String transformQueryForExternalDatabase(
    const IAST & query,
    const NamesAndTypesList & available_columns,
    const String & database,
    const String & table,
    const Context & context);

}
