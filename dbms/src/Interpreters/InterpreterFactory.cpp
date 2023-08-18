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

#include <Common/typeid_cast.h>
#include <Interpreters/InterpreterAlterQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDBGInvokeQuery.h>
#include <Interpreters/InterpreterDescribeQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterManageQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterShowProcesslistQuery.h>
#include <Interpreters/InterpreterShowTablesQuery.h>
#include <Interpreters/InterpreterUseQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDBGInvokeQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTManageQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTShowTablesQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>


namespace DB
{
namespace ErrorCodes
{
extern const int READONLY;
extern const int UNKNOWN_TYPE_OF_QUERY;
} // namespace ErrorCodes


static void throwIfReadOnly(Context & context)
{
    if (context.getSettingsRef().readonly)
    {
        throw Exception("Cannot execute query in readonly mode", ErrorCodes::READONLY);
    }
}


std::unique_ptr<IInterpreter> InterpreterFactory::get(
    ASTPtr & query,
    Context & context,
    QueryProcessingStage::Enum stage)
{
    if (typeid_cast<ASTSelectQuery *>(query.get()))
    {
        return std::make_unique<InterpreterSelectQuery>(query, context, Names{}, stage);
    }
    else if (typeid_cast<ASTSelectWithUnionQuery *>(query.get()))
    {
        return std::make_unique<InterpreterSelectWithUnionQuery>(query, context, Names{}, stage);
    }
    else if (typeid_cast<ASTInsertQuery *>(query.get()))
    {
        /// readonly is checked inside InterpreterInsertQuery
        return std::make_unique<InterpreterInsertQuery>(query, context, false);
    }
    else if (typeid_cast<ASTCreateQuery *>(query.get()))
    {
        /// readonly is checked inside InterpreterCreateQuery
        return std::make_unique<InterpreterCreateQuery>(query, context);
    }
    else if (typeid_cast<ASTDropQuery *>(query.get()))
    {
        /// readonly is checked inside InterpreterDropQuery
        return std::make_unique<InterpreterDropQuery>(query, context);
    }
    else if (typeid_cast<ASTRenameQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterRenameQuery>(query, context, context.getCurrentQueryId());
    }
    else if (typeid_cast<ASTShowTablesQuery *>(query.get()))
    {
        return std::make_unique<InterpreterShowTablesQuery>(query, context);
    }
    else if (typeid_cast<ASTUseQuery *>(query.get()))
    {
        return std::make_unique<InterpreterUseQuery>(query, context);
    }
    else if (typeid_cast<ASTSetQuery *>(query.get()))
    {
        /// readonly is checked inside InterpreterSetQuery
        return std::make_unique<InterpreterSetQuery>(query, context);
    }
    else if (typeid_cast<ASTExistsQuery *>(query.get()))
    {
        return std::make_unique<InterpreterExistsQuery>(query, context);
    }
    else if (typeid_cast<ASTDescribeQuery *>(query.get()))
    {
        return std::make_unique<InterpreterDescribeQuery>(query, context);
    }
    else if (typeid_cast<ASTShowProcesslistQuery *>(query.get()))
    {
        return std::make_unique<InterpreterShowProcesslistQuery>(query, context);
    }
    else if (typeid_cast<ASTAlterQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterAlterQuery>(query, context);
    }
    else if (typeid_cast<ASTDBGInvokeQuery *>(query.get()))
    {
        return std::make_unique<InterpreterDBGInvokeQuery>(query, context);
    }

    else if (typeid_cast<ASTManageQuery *>(query.get()))
    {
        throwIfReadOnly(context);
        return std::make_unique<InterpreterManageQuery>(query, context);
    }
    else
        throw Exception("Unknown type of query: " + query->getID(), ErrorCodes::UNKNOWN_TYPE_OF_QUERY);
}
} // namespace DB
