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

#include <Columns/IColumn.h>

#include <string>


namespace DB
{

struct DictionaryStructure;
class WriteBuffer;


/** Builds a query to load data from external database.
  */
struct ExternalQueryBuilder
{
    const DictionaryStructure & dict_struct;
    const std::string & db;
    const std::string & table;
    const std::string & where;

    /// Method to quote identifiers.
    /// NOTE There could be differences in escaping rules inside quotes. Escaping rules may not match that required by specific external DBMS.
    enum QuotingStyle
    {
        None, /// Write as-is, without quotes.
        Backticks, /// `mysql` style
        DoubleQuotes /// "postgres" style
    };

    QuotingStyle quoting_style;


    ExternalQueryBuilder(
        const DictionaryStructure & dict_struct,
        const std::string & db,
        const std::string & table,
        const std::string & where,
        QuotingStyle quoting_style);

    /** Generate a query to load all data. */
    std::string composeLoadAllQuery() const;

    /** Generate a query to load data after certain time point*/
    std::string composeUpdateQuery(const std::string & update_field, const std::string & time_point) const;

    /** Generate a query to load data by set of UInt64 keys. */
    std::string composeLoadIdsQuery(const std::vector<UInt64> & ids);

    /** Generate a query to load data by set of composite keys.
      * There are two methods of specification of composite keys in WHERE:
      * 1. (x = c11 AND y = c12) OR (x = c21 AND y = c22) ...
      * 2. (x, y) IN ((c11, c12), (c21, c22), ...)
      */
    enum LoadKeysMethod
    {
        AND_OR_CHAIN,
        IN_WITH_TUPLES,
    };

    std::string composeLoadKeysQuery(
        const Columns & key_columns,
        const std::vector<size_t> & requested_rows,
        LoadKeysMethod method);


private:
    /// Expression in form (x = c1 AND y = c2 ...)
    void composeKeyCondition(const Columns & key_columns, const size_t row, WriteBuffer & out) const;

    /// Expression in form (x, y, ...)
    std::string composeKeyTupleDefinition() const;

    /// Expression in form (c1, c2, ...)
    void composeKeyTuple(const Columns & key_columns, const size_t row, WriteBuffer & out) const;

    /// Write string with specified quoting style.
    void writeQuoted(const std::string & s, WriteBuffer & out) const;
};

} // namespace DB
