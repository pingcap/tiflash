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

#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <Common/config.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypesNumber.h>
#include <Dictionaries/ClickHouseDictionarySource.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/ExecutableDictionarySource.h>
#include <Dictionaries/FileDictionarySource.h>
#include <Dictionaries/LibraryDictionarySource.h>
#include <IO/HTTPCommon.h>
#include <Poco/Logger.h>
#include <common/logger_useful.h>

#include <memory>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
extern const int UNKNOWN_ELEMENT_IN_CONFIG;
extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
extern const int LOGICAL_ERROR;
extern const int SUPPORT_IS_DISABLED;
} // namespace ErrorCodes

namespace
{

Block createSampleBlock(const DictionaryStructure & dict_struct)
{
    Block block;

    if (dict_struct.id)
        block.insert(ColumnWithTypeAndName{
            ColumnUInt64::create(1, 0),
            std::make_shared<DataTypeUInt64>(),
            dict_struct.id->name});

    if (dict_struct.key)
    {
        for (const auto & attribute : *dict_struct.key)
        {
            auto column = attribute.type->createColumn();
            column->insertDefault();

            block.insert(ColumnWithTypeAndName{std::move(column), attribute.type, attribute.name});
        }
    }

    if (dict_struct.range_min)
        for (const auto & attribute : {dict_struct.range_min, dict_struct.range_max})
            block.insert(
                ColumnWithTypeAndName{ColumnUInt16::create(1, 0), std::make_shared<DataTypeDate>(), attribute->name});

    for (const auto & attribute : dict_struct.attributes)
    {
        auto column = attribute.type->createColumn();
        column->insert(attribute.null_value);

        block.insert(ColumnWithTypeAndName{std::move(column), attribute.type, attribute.name});
    }

    return block;
}

} // namespace


DictionarySourceFactory::DictionarySourceFactory()
    : log(&Poco::Logger::get("DictionarySourceFactory"))
{}

void DictionarySourceFactory::registerSource(const std::string & source_type, Creator create_source)
{
    LOG_DEBUG(log, "Register dictionary source type `" + source_type + "`");
    if (!registered_sources.emplace(source_type, std::move(create_source)).second)
        throw Exception(
            "DictionarySourceFactory: the source name '" + source_type + "' is not unique",
            ErrorCodes::LOGICAL_ERROR);
}

DictionarySourcePtr DictionarySourceFactory::create(
    const std::string & name,
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_prefix,
    const DictionaryStructure & dict_struct,
    Context & context) const
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);
    if (keys.size() != 1)
        throw Exception{
            name + ": element dictionary.source should have exactly one child element",
            ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG};

    auto sample_block = createSampleBlock(dict_struct);

    const auto & source_type = keys.front();

    if ("file" == source_type)
    {
        if (dict_struct.has_expressions)
            throw Exception{
                "Dictionary source of type `file` does not support attribute expressions",
                ErrorCodes::LOGICAL_ERROR};

        const auto filename = config.getString(config_prefix + ".file.path");
        const auto format = config.getString(config_prefix + ".file.format");
        return std::make_unique<FileDictionarySource>(filename, format, sample_block, context);
    }
    else if ("clickhouse" == source_type)
    {
        return std::make_unique<ClickHouseDictionarySource>(
            dict_struct,
            config,
            config_prefix + ".clickhouse",
            sample_block,
            context);
    }
    else if ("executable" == source_type)
    {
        if (dict_struct.has_expressions)
            throw Exception{
                "Dictionary source of type `executable` does not support attribute expressions",
                ErrorCodes::LOGICAL_ERROR};

        return std::make_unique<ExecutableDictionarySource>(
            dict_struct,
            config,
            config_prefix + ".executable",
            sample_block,
            context);
    }
    else if ("library" == source_type)
    {
        return std::make_unique<LibraryDictionarySource>(
            dict_struct,
            config,
            config_prefix + ".library",
            sample_block,
            context);
    }
    else
    {
        const auto found = registered_sources.find(source_type);
        if (found != registered_sources.end())
        {
            const auto & create_source = found->second;
            return create_source(dict_struct, config, config_prefix, sample_block, context);
        }
    }

    throw Exception{name + ": unknown dictionary source type: " + source_type, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG};
}

} // namespace DB
