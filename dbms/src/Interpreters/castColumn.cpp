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

#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/castColumn.h>


namespace DB
{
ColumnPtr castColumnImpl(
    const ColumnWithTypeAndName & arg,
    const DataTypePtr & type,
    const Context & context,
    const String & func_name)
{
    if (arg.type->equals(*type))
        return arg.column;

    Block temporary_block{
        arg,
        {DataTypeString().createColumnConst(arg.column->size(), type->getName()),
         std::make_shared<DataTypeString>(),
         ""},
        {nullptr, type, ""}};

    FunctionBuilderPtr func_builder_cast = FunctionFactory::instance().get(func_name, context);

    ColumnsWithTypeAndName arguments{temporary_block.getByPosition(0), temporary_block.getByPosition(1)};
    auto func_cast = func_builder_cast->build(arguments);

    func_cast->execute(temporary_block, {0, 1}, 2);
    return temporary_block.getByPosition(2).column;
}

ColumnPtr castColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, const Context & context)
{
    return castColumnImpl(arg, type, context, "CAST");
}

ColumnPtr tiDBCastColumn(const ColumnWithTypeAndName & arg, const DataTypePtr & type, const Context & context)
{
    return castColumnImpl(arg, type, context, "tidb_cast");
}


} // namespace DB
