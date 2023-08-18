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

#include <Core/Names.h>
#include <Core/Types.h>

#include <map>


namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
} // namespace Poco


namespace DB
{
/** Apply substitutions from the macros in config to the string.
  */
class Macros
{
public:
    Macros() = default;
    Macros(const Poco::Util::AbstractConfiguration & config, const String & key);

    /** Replace the substring of the form {macro_name} with the value for macro_name, obtained from the config file.
      * level - the level of recursion.
      */
    String expand(const String & s, size_t level = 0) const;

    /** Apply expand for the list.
      */
    Names expand(const Names & source_names, size_t level = 0) const;

    using MacroMap = std::map<String, String>;
    MacroMap getMacroMap() const { return macros; }

private:
    MacroMap macros;
};


} // namespace DB
