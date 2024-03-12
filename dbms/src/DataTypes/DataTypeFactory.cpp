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
#include <DataTypes/DataTypeFactory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Poco/String.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int UNKNOWN_TYPE;
extern const int ILLEGAL_SYNTAX_FOR_DATA_TYPE;
extern const int UNEXPECTED_AST_STRUCTURE;
extern const int DATA_TYPE_CANNOT_HAVE_ARGUMENTS;
} // namespace ErrorCodes


DataTypePtr DataTypeFactory::get(const String & full_name) const
{
    ParserIdentifierWithOptionalParameters parser;
    ASTPtr ast = parseQuery(parser, full_name.data(), full_name.data() + full_name.size(), "data type", 0);
    return get(ast);
}
// DataTypeFactory is a Singleton, so need to be protected by lock.
DataTypePtr DataTypeFactory::getOrSet(const String & full_name)
{
    {
        std::shared_lock lock(rw_lock);
        auto it = fullname_types.find(full_name);
        if (it != fullname_types.end())
        {
            return it->second;
        }
    }
    ParserIdentifierWithOptionalParameters parser;
    ASTPtr ast = parseQuery(parser, full_name.data(), full_name.data() + full_name.size(), "data type", 0);
    DataTypePtr datatype_ptr = get(ast);
    // avoid big hashmap in rare cases.
    std::unique_lock lock(rw_lock);
    if (fullname_types.size() < MAX_FULLNAME_TYPES)
    {
        // DataTypeEnum may generate too many full_name, so just skip inserting DataTypeEnum into fullname_types when
        // the capacity limit is almost reached, which ensures that most datatypes can be cached.
        if (fullname_types.size() > FULLNAME_TYPES_HIGH_WATER_MARK
            && (datatype_ptr->getTypeId() == TypeIndex::Enum8 || datatype_ptr->getTypeId() == TypeIndex::Enum16))
        {
            return datatype_ptr;
        }
        fullname_types.emplace(full_name, datatype_ptr);
    }
    return datatype_ptr;
}

size_t DataTypeFactory::getFullNameCacheSize() const
{
    std::shared_lock lock(rw_lock);
    return fullname_types.size();
}

DataTypePtr DataTypeFactory::get(const ASTPtr & ast) const
{
    if (const auto * func = typeid_cast<const ASTFunction *>(ast.get()))
    {
        if (func->parameters)
            throw Exception(
                "Data type cannot have multiple parenthesed parameters.",
                ErrorCodes::ILLEGAL_SYNTAX_FOR_DATA_TYPE);
        return get(func->name, func->arguments);
    }

    if (const auto * ident = typeid_cast<const ASTIdentifier *>(ast.get()))
    {
        return get(ident->name, {});
    }

    if (const auto * lit = typeid_cast<const ASTLiteral *>(ast.get()))
    {
        if (lit->value.isNull())
            return get("Null", {});
    }

    throw Exception("Unexpected AST element for data type.", ErrorCodes::UNEXPECTED_AST_STRUCTURE);
}

DataTypePtr DataTypeFactory::get(const String & family_name, const ASTPtr & parameters) const
{
    {
        auto it = data_types.find(family_name);
        if (data_types.end() != it)
            return it->second(parameters);
    }

    {
        String family_name_lowercase = Poco::toLower(family_name);
        auto it = case_insensitive_data_types.find(family_name_lowercase);
        if (case_insensitive_data_types.end() != it)
            return it->second(parameters);
    }

    throw Exception("Unknown data type family: " + family_name, ErrorCodes::UNKNOWN_TYPE);
}


void DataTypeFactory::registerDataType(
    const String & family_name,
    Creator creator,
    CaseSensitiveness case_sensitiveness)
{
    if (creator == nullptr)
        throw Exception(
            "DataTypeFactory: the data type family " + family_name + " has been provided a null constructor",
            ErrorCodes::LOGICAL_ERROR);

    if (!data_types.emplace(family_name, creator).second)
        throw Exception(
            "DataTypeFactory: the data type family name '" + family_name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);

    String family_name_lowercase = Poco::toLower(family_name);

    if (case_sensitiveness == CaseInsensitive
        && !case_insensitive_data_types.emplace(family_name_lowercase, creator).second)
        throw Exception(
            "DataTypeFactory: the case insensitive data type family name '" + family_name + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}


void DataTypeFactory::registerSimpleDataType(
    const String & name,
    SimpleCreator creator,
    CaseSensitiveness case_sensitiveness)
{
    if (creator == nullptr)
        throw Exception(
            "DataTypeFactory: the data type " + name + " has been provided a null constructor",
            ErrorCodes::LOGICAL_ERROR);

    registerDataType(
        name,
        [name, creator](const ASTPtr & ast) {
            if (ast)
                throw Exception(
                    "Data type " + name + " cannot have arguments",
                    ErrorCodes::DATA_TYPE_CANNOT_HAVE_ARGUMENTS);
            return creator();
        },
        case_sensitiveness);
}


void registerDataTypeNumbers(DataTypeFactory & factory);
void registerDataTypeDate(DataTypeFactory & factory);
void registerDataTypeDateTime(DataTypeFactory & factory);
void registerDataTypeMyDateTime(DataTypeFactory & factory);
void registerDataTypeMyDate(DataTypeFactory & factory);
void registerDataTypeString(DataTypeFactory & factory);
void registerDataTypeFixedString(DataTypeFactory & factory);
void registerDataTypeDecimal(DataTypeFactory & factory);
void registerDataTypeEnum(DataTypeFactory & factory);
void registerDataTypeArray(DataTypeFactory & factory);
void registerDataTypeTuple(DataTypeFactory & factory);
void registerDataTypeNullable(DataTypeFactory & factory);
void registerDataTypeNothing(DataTypeFactory & factory);
void registerDataTypeUUID(DataTypeFactory & factory);
void registerDataTypeNested(DataTypeFactory & factory);
void registerDataTypeInterval(DataTypeFactory & factory);
void registerDataTypeDuration(DataTypeFactory & factory);


DataTypeFactory::DataTypeFactory()
{
    registerDataTypeNumbers(*this);
    registerDataTypeDate(*this);
    registerDataTypeDateTime(*this);
    registerDataTypeMyDateTime(*this);
    registerDataTypeString(*this);
    registerDataTypeFixedString(*this);
    registerDataTypeDecimal(*this);
    registerDataTypeEnum(*this);
    registerDataTypeArray(*this);
    registerDataTypeTuple(*this);
    registerDataTypeNullable(*this);
    registerDataTypeNothing(*this);
    registerDataTypeUUID(*this);
    registerDataTypeNested(*this);
    registerDataTypeInterval(*this);
    registerDataTypeMyDate(*this);
    registerDataTypeDuration(*this);
}

} // namespace DB
