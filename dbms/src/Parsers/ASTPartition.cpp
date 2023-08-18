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

#include <IO/WriteHelpers.h>
#include <Parsers/ASTPartition.h>

namespace DB
{

String ASTPartition::getID() const
{
    if (value)
        return "Partition";
    else
        return "Partition_ID_" + id;
}

ASTPtr ASTPartition::clone() const
{
    auto res = std::make_shared<ASTPartition>(*this);
    res->children.clear();

    if (value)
    {
        res->value = value->clone();
        res->children.push_back(res->value);
    }

    return res;
}

void ASTPartition::formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    if (value)
    {
        value->formatImpl(settings, state, frame);
    }
    else
    {
        settings.ostr << (settings.hilite ? hilite_keyword : "") << "ID " << (settings.hilite ? hilite_none : "");
        WriteBufferFromOwnString id_buf;
        writeQuoted(id, id_buf);
        settings.ostr << id_buf.str();
    }
}

} // namespace DB
