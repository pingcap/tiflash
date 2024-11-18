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

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>


namespace DB
{
class ASTStorage : public IAST
{
public:
    ASTFunction * engine = nullptr;
    IAST * partition_by = nullptr;
    IAST * order_by = nullptr;
    IAST * sample_by = nullptr;
    ASTSetQuery * settings = nullptr;

    String getID() const override { return "Storage definition"; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTStorage>(*this);
        res->children.clear();

        if (engine)
            res->set(res->engine, engine->clone());
        if (partition_by)
            res->set(res->partition_by, partition_by->clone());
        if (order_by)
            res->set(res->order_by, order_by->clone());
        if (sample_by)
            res->set(res->sample_by, sample_by->clone());
        if (settings)
            res->set(res->settings, settings->clone());

        return res;
    }

    void formatImpl(const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override
    {
        if (engine)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ENGINE" << (s.hilite ? hilite_none : "")
                   << " = ";
            engine->formatImpl(s, state, frame);
        }
        if (partition_by)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "PARTITION BY "
                   << (s.hilite ? hilite_none : "");
            partition_by->formatImpl(s, state, frame);
        }
        if (order_by)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "ORDER BY " << (s.hilite ? hilite_none : "");
            order_by->formatImpl(s, state, frame);
        }
        if (sample_by)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SAMPLE BY " << (s.hilite ? hilite_none : "");
            sample_by->formatImpl(s, state, frame);
        }
        if (settings)
        {
            s.ostr << (s.hilite ? hilite_keyword : "") << s.nl_or_ws << "SETTINGS " << (s.hilite ? hilite_none : "");
            settings->formatImpl(s, state, frame);
        }
    }
};


/// CREATE TABLE or ATTACH TABLE query
class ASTCreateQuery : public ASTQueryWithOutput
{
public:
    bool attach{false}; /// Query ATTACH TABLE, not CREATE TABLE.
    bool if_not_exists{false};
    String database;
    String table;
    ASTExpressionList * columns = nullptr;
    ASTStorage * storage = nullptr;

    /** Get the text that identifies this element. */
    String getID() const override { return (attach ? "AttachQuery_" : "CreateQuery_") + database + "_" + table; }

    ASTPtr clone() const override
    {
        auto res = std::make_shared<ASTCreateQuery>(*this);
        res->children.clear();

        if (columns)
            res->set(res->columns, columns->clone());
        if (storage)
            res->set(res->storage, storage->clone());

        cloneOutputOptions(*res);

        return res;
    }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
    {
        frame.need_parens = false;

        if (!database.empty() && table.empty())
        {
            settings.ostr << (settings.hilite ? hilite_keyword : "")
                          << (attach ? "ATTACH DATABASE " : "CREATE DATABASE ")
                          << (if_not_exists ? "IF NOT EXISTS " : "") << (settings.hilite ? hilite_none : "")
                          << backQuoteIfNeed(database);

            if (storage)
                storage->formatImpl(settings, state, frame);

            return;
        }

        {
            std::string what = "TABLE";
            settings.ostr << (settings.hilite ? hilite_keyword : "") << (attach ? "ATTACH " : "CREATE ") << what << " "
                          << (if_not_exists ? "IF NOT EXISTS " : "") << (settings.hilite ? hilite_none : "")
                          << (!database.empty() ? backQuoteIfNeed(database) + "." : "") << backQuoteIfNeed(table);
        }

        if (columns)
        {
            settings.ostr << (settings.one_line ? " (" : "\n(");
            FormatStateStacked frame_nested = frame;
            ++frame_nested.indent;
            columns->formatImpl(settings, state, frame_nested);
            settings.ostr << (settings.one_line ? ")" : "\n)");
        }

        if (storage)
            storage->formatImpl(settings, state, frame);

    }
};

} // namespace DB
