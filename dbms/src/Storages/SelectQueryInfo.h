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

#include <memory>
#include <string>
#include <unordered_map>

namespace DB
{

class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class Set;
using SetPtr = std::shared_ptr<Set>;

/// Information about calculated sets in right hand side of IN.
using PreparedSets = std::unordered_map<IAST *, SetPtr>;

struct MvccQueryInfo;
struct DAGQueryInfo;


/** Query along with some additional data,
  *  that can be used during query processing
  *  inside storage engines.
  */
struct SelectQueryInfo
{
    ASTPtr query;

    /// Prepared sets are used for indices by storage engine.
    /// Example: x IN (1, 2, 3)
    PreparedSets sets;

    std::unique_ptr<MvccQueryInfo> mvcc_query_info;

    std::unique_ptr<DAGQueryInfo> dag_query;

    std::string req_id;
    bool keep_order = true;
    bool is_fast_scan = false;

    SelectQueryInfo();
    ~SelectQueryInfo();

    // support copying and moving
    SelectQueryInfo(const SelectQueryInfo & rhs);
    SelectQueryInfo(SelectQueryInfo && rhs) noexcept;

    bool fromAST() const { return dag_query == nullptr; };
};

} // namespace DB
