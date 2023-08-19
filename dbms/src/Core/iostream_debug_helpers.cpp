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

#include "iostream_debug_helpers.h"

#include <Common/COWPtr.h>
#include <Common/FieldVisitors.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Parsers/IAST.h>
#include <Storages/IStorage.h>

#include <iostream>


namespace DB
{
std::ostream & operator<<(std::ostream & stream, const IBlockInputStream & what)
{
    stream << "IBlockInputStream(name = " << what.getName() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Field & what)
{
    stream << applyVisitor(FieldVisitorDump(), what);
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const NameAndTypePair & what)
{
    stream << "NameAndTypePair(name = " << what.name << ", type = " << what.type << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IDataType & what)
{
    stream << "IDataType(name = " << what.getName() << ", default = " << what.getDefault() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IStorage & what)
{
    stream << "IStorage(name = " << what.getName() << ", tableName = " << what.getTableName() << ") {"
           << what.getColumns().getAllPhysical().toString()
           << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const TableStructureReadLock &)
{
    stream << "TableStructureReadLock()";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IFunctionBuilder & what)
{
    stream << "IFunction(name = " << what.getName() << ", variadic = " << what.isVariadic() << ", args = "
           << what.getNumberOfArguments() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Block & what)
{
    stream << "Block("
           << "num_columns = " << what.columns()
           << "){" << what.dumpStructure() << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const ColumnWithTypeAndName & what)
{
    stream << "ColumnWithTypeAndName(name = " << what.name << ", type = " << what.type << ", column = ";
    return dumpValue(stream, what.column) << ")";
}

std::ostream & operator<<(std::ostream & stream, const IColumn & what)
{
    stream << "IColumn(" << what.dumpStructure() << ")";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const Connection::Packet & what)
{
    stream << "Connection::Packet("
           << "type = " << what.type;
    // types description: Core/Protocol.h
    if (what.exception)
        stream << "exception = " << what.exception.get();
    // TODO: profile_info
    stream << ") {" << what.block << "}";
    return stream;
}

std::ostream & operator<<(std::ostream & stream, const IAST & what)
{
    stream << "IAST{";
    what.dumpTree(stream);
    stream << "}";
    return stream;
}

} // namespace DB
