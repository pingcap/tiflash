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

#include <Core/SortDescription.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <TiDB/Collation/Collator.h>

#include <sstream>


namespace DB
{
std::string SortColumnDescription::getID() const
{
    WriteBufferFromOwnString out;
    out << column_name << ", " << column_number << ", " << direction << ", " << nulls_direction;
    if (collator)
        out << ", collation locale: " << collator->getCollatorId();
    return out.str();
}

} // namespace DB
