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

#include <Functions/re2Util.h>

namespace DB
{
namespace re2Util
{
re2_st::RE2::Options getDefaultRe2Options()
{
    re2_st::RE2::Options options(re2_st::RE2::CannedOptions::DefaultOptions);
    options.set_case_sensitive(true);
    options.set_one_line(true);
    options.set_dot_nl(false);
    return options;
}

// If characters specifying contradictory options are specified
// within match_type, the rightmost one takes precedence.
String getRE2ModeModifiers(const std::string & match_type, const TiDB::TiDBCollatorPtr collator)
{
    /// for regexp only ci/cs is supported
    re2_st::RE2::Options options = getDefaultRe2Options();
    if (collator != nullptr && collator->isCI())
        options.set_case_sensitive(false);

    /// match_type can overwrite collator
    if (!match_type.empty())
    {
        for (const auto & c : match_type)
        {
            switch (c)
            {
            case 'i':
                /// according to MySQL doc: if either argument is a binary string, the arguments are handled in
                /// case-sensitive fashion as binary strings, even if match_type contains the i character.
                /// However, test in MySQL 8.0.25 shows that i flag still take affect even if the collation is binary,
                if (collator == nullptr || !collator->isBinary())
                    options.set_case_sensitive(false);
                break;
            case 'c':
                options.set_case_sensitive(true);
                break;
            case 's':
                options.set_dot_nl(true);
                break;
            case 'm':
                options.set_one_line(false);
                break;
            default:
                throw Exception("Invalid match type in regexp related functions.");
            }
        }
    }
    if (!options.one_line() || options.dot_nl() || !options.case_sensitive())
    {
        String mode_modifiers("(?");
        if (!options.one_line())
            mode_modifiers += "m";
        if (!options.case_sensitive())
            mode_modifiers += "i";
        if (options.dot_nl())
            mode_modifiers += "s";
        mode_modifiers += ")";
        return mode_modifiers;
    }
    else
        return "";
}
} // namespace re2Util
} // namespace DB
