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

#include <string>


namespace DB
{
class IAST;

/// Get the cluster name from AST.
/** The name of the cluster is the name of the tag in the xml configuration.
  * Usually it is parsed as an identifier. That is, it can contain underscores, but can not contain hyphens,
  *  provided that the identifier is not in backquotes.
  * But in xml, as a tag name, it's more common to use hyphens.
  * This name will be parsed as an expression with an operator minus - not at all what you need.
  * Therefore, consider this case separately.
  */
std::string getClusterName(const IAST & node);

} // namespace DB
