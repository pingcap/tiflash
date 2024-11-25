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

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/FieldVisitors.h>
#include <Common/FmtUtils.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{
namespace ErrorCodes
{
extern const int SYNTAX_ERROR;
extern const int BAD_ARGUMENTS;
extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes


std::string DataTypeAggregateFunction::getName() const
{
    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("AggregateFunction({}", function->getName());

    if (!parameters.empty())
    {
        fmt_buf.append("(");
        fmt_buf.joinStr(
            parameters.cbegin(),
            parameters.cend(),
            [](const auto & arg, FmtBuffer & fb) { fb.append(applyVisitor(DB::FieldVisitorToString(), arg)); },
            ", ");
        fmt_buf.append(")");
    }

    for (const auto & argument_type : argument_types)
        fmt_buf.fmtAppend(", {}", argument_type->getName());

    fmt_buf.append(")");
    return fmt_buf.toString();
}

void DataTypeAggregateFunction::serializeBinary(const Field & field, WriteBuffer & ostr) const
{
    const auto & s = get<const String &>(field);
    writeVarUInt(s.size(), ostr);
    writeString(s, ostr);
}

void DataTypeAggregateFunction::deserializeBinary(Field & field, ReadBuffer & istr) const
{
    UInt64 size;
    readVarUInt(size, istr);
    field = String();
    auto & s = get<String &>(field);
    s.resize(size);
    istr.readStrict(&s[0], size);
}

void DataTypeAggregateFunction::serializeBinary(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    function->serialize(static_cast<const ColumnAggregateFunction &>(column).getData()[row_num], ostr);
}

void DataTypeAggregateFunction::deserializeBinary(IColumn & column, ReadBuffer & istr) const
{
    auto & column_concrete = static_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alloc(size_of_state);

    function->create(place);
    try
    {
        function->deserialize(place, istr, &arena);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    column_concrete.getData().push_back(place);
}

void DataTypeAggregateFunction::serializeBinaryBulk(
    const IColumn & column,
    WriteBuffer & ostr,
    size_t offset,
    size_t limit) const
{
    const ColumnAggregateFunction & real_column = typeid_cast<const ColumnAggregateFunction &>(column);
    const ColumnAggregateFunction::Container & vec = real_column.getData();

    ColumnAggregateFunction::Container::const_iterator it = vec.begin() + offset;
    ColumnAggregateFunction::Container::const_iterator end = limit ? it + limit : vec.end();

    if (end > vec.end())
        end = vec.end();

    for (; it != end; ++it)
        function->serialize(*it, ostr);
}

void DataTypeAggregateFunction::deserializeBinaryBulk(
    IColumn & column,
    ReadBuffer & istr,
    size_t limit,
    double /*avg_value_size_hint*/,
    const IColumn::Filter * filter) const
{
    ColumnAggregateFunction & real_column = typeid_cast<ColumnAggregateFunction &>(column);
    ColumnAggregateFunction::Container & vec = real_column.getData();

    Arena & arena = real_column.createOrGetArena();
    real_column.set(function);
    vec.reserve(vec.size() + limit);

    size_t size_of_state = function->sizeOfData();

    for (size_t i = 0; i < limit; ++i)
    {
        if (istr.eof())
            break;

        // Actually, there is no need to deserialize each row when filter is provided.
        // But since we do not know the size of serialized state, we have to deserialize it.
        // TODO: Optimize this.
        AggregateDataPtr place = arena.alloc(size_of_state);

        function->create(place);

        try
        {
            function->deserialize(place, istr, &arena);
        }
        catch (...)
        {
            function->destroy(place);
            throw;
        }

        if (filter && !(*filter)[i])
            function->destroy(place);
        else
            vec.push_back(place);
    }
}

static String serializeToString(const AggregateFunctionPtr & function, const IColumn & column, size_t row_num)
{
    WriteBufferFromOwnString buffer;
    function->serialize(static_cast<const ColumnAggregateFunction &>(column).getData()[row_num], buffer);
    return buffer.str();
}

static void deserializeFromString(const AggregateFunctionPtr & function, IColumn & column, const String & s)
{
    auto & column_concrete = static_cast<ColumnAggregateFunction &>(column);

    Arena & arena = column_concrete.createOrGetArena();
    size_t size_of_state = function->sizeOfData();
    AggregateDataPtr place = arena.alloc(size_of_state);

    function->create(place);

    try
    {
        ReadBufferFromString istr(s);
        function->deserialize(place, istr, &arena);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    column_concrete.getData().push_back(place);
}

void DataTypeAggregateFunction::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeString(serializeToString(function, column, row_num), ostr);
}


void DataTypeAggregateFunction::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeEscapedString(serializeToString(function, column, row_num), ostr);
}


void DataTypeAggregateFunction::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
    String s;
    readEscapedString(s, istr);
    deserializeFromString(function, column, s);
}


void DataTypeAggregateFunction::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
    writeQuotedString(serializeToString(function, column, row_num), ostr);
}


void DataTypeAggregateFunction::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
    String s;
    readQuotedStringWithSQLStyle(s, istr);
    deserializeFromString(function, column, s);
}


void DataTypeAggregateFunction::serializeTextJSON(
    const IColumn & column,
    size_t row_num,
    WriteBuffer & ostr,
    const FormatSettingsJSON &) const
{
    writeJSONString(serializeToString(function, column, row_num), ostr);
}


void DataTypeAggregateFunction::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
    String s;
    readJSONString(s, istr);
    deserializeFromString(function, column, s);
}


MutableColumnPtr DataTypeAggregateFunction::createColumn() const
{
    return ColumnAggregateFunction::create(function);
}


/// Create empty state
Field DataTypeAggregateFunction::getDefault() const
{
    Field field = String();

    PODArrayWithStackMemory<char, 16> place_buffer(function->sizeOfData());
    AggregateDataPtr place = place_buffer.data();

    function->create(place);

    try
    {
        WriteBufferFromString buffer_from_field(field.get<String &>());
        function->serialize(place, buffer_from_field);
    }
    catch (...)
    {
        function->destroy(place);
        throw;
    }

    function->destroy(place);

    return field;
}

bool DataTypeAggregateFunction::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this) && getName() == rhs.getName();
}

/// This function is only used in test
static DataTypePtr create(const ASTPtr & arguments)
{
    String function_name;
    AggregateFunctionPtr function;
    DataTypes argument_types;
    Array params_row;

    if (!arguments || arguments->children.empty())
        throw Exception(
            "Data type AggregateFunction requires parameters: "
            "name of aggregate function and list of data types for arguments",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (const auto * parametric = typeid_cast<const ASTFunction *>(arguments->children[0].get()))
    {
        if (parametric->parameters)
            throw Exception("Unexpected level of parameters to aggregate function", ErrorCodes::SYNTAX_ERROR);
        function_name = parametric->name;

        const ASTs & parameters = typeid_cast<const ASTExpressionList &>(*parametric->arguments).children;
        params_row.resize(parameters.size());

        for (size_t i = 0; i < parameters.size(); ++i)
        {
            const auto * lit = typeid_cast<const ASTLiteral *>(parameters[i].get());
            if (!lit)
                throw Exception(
                    "Parameters to aggregate functions must be literals",
                    ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS);

            params_row[i] = lit->value;
        }
    }
    else if (const ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(arguments->children[0].get()))
    {
        function_name = identifier->name;
    }
    else if (typeid_cast<ASTLiteral *>(arguments->children[0].get()))
    {
        throw Exception(
            "Aggregate function name for data type AggregateFunction must be passed as identifier (without quotes) or "
            "function",
            ErrorCodes::BAD_ARGUMENTS);
    }
    else
        throw Exception(
            "Unexpected AST element passed as aggregate function name for data type AggregateFunction. Must be "
            "identifier or function.",
            ErrorCodes::BAD_ARGUMENTS);

    for (size_t i = 1; i < arguments->children.size(); ++i)
        argument_types.push_back(DataTypeFactory::instance().get(arguments->children[i]));

    if (function_name.empty())
        throw Exception("Logical error: empty name of aggregate function passed", ErrorCodes::LOGICAL_ERROR);

    /// This is for test usage only
    auto context_ptr = Context::createGlobal();
    function = AggregateFunctionFactory::instance().get(*context_ptr, function_name, argument_types, params_row);
    return std::make_shared<DataTypeAggregateFunction>(function, argument_types, params_row);
}

void registerDataTypeAggregateFunction(DataTypeFactory & factory)
{
    factory.registerDataType("AggregateFunction", create);
}

} // namespace DB
