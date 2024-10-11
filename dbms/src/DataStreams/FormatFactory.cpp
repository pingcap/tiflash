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

#include <Common/config.h>
#include <DataStreams/BinaryRowInputStream.h>
#include <DataStreams/BinaryRowOutputStream.h>
#include <DataStreams/BlockInputStreamFromRowInputStream.h>
#include <DataStreams/BlockOutputStreamFromRowOutputStream.h>
#include <DataStreams/FormatFactory.h>
#include <DataStreams/JSONCompactRowOutputStream.h>
#include <DataStreams/JSONEachRowRowInputStream.h>
#include <DataStreams/JSONEachRowRowOutputStream.h>
#include <DataStreams/JSONRowOutputStream.h>
#include <DataStreams/MaterializingBlockOutputStream.h>
#include <DataStreams/NativeBlockInputStream.h>
#include <DataStreams/NativeBlockOutputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <DataStreams/SquashingBlockOutputStream.h>
#include <DataTypes/FormatSettingsJSON.h>
#include <Interpreters/Context.h>
#include <boost_wrapper/string.h>

namespace DB
{

namespace ErrorCodes
{
extern const int FORMAT_IS_NOT_SUITABLE_FOR_INPUT;
extern const int UNKNOWN_FORMAT;
} // namespace ErrorCodes


BlockInputStreamPtr FormatFactory::getInput(
    const String & name,
    ReadBuffer & buf,
    const Block & sample,
    const Context & context,
    size_t max_block_size)
{
    const Settings & settings = context.getSettingsRef();

    auto wrap_row_stream = [&](auto && row_stream) {
        return std::make_shared<BlockInputStreamFromRowInputStream>(
            std::forward<decltype(row_stream)>(row_stream),
            sample,
            max_block_size,
            0,
            0);
    };

    if (name == "Native")
    {
        return std::make_shared<NativeBlockInputStream>(buf, sample, 0);
    }
    else if (name == "RowBinary")
    {
        return wrap_row_stream(std::make_shared<BinaryRowInputStream>(buf, sample));
    }
    else if (name == "JSONEachRow")
    {
        return wrap_row_stream(
            std::make_shared<JSONEachRowRowInputStream>(buf, sample, settings.input_format_skip_unknown_fields));
    }
    else if (name == "Null" || name == "JSON" || name == "JSONCompact")
    {
        throw Exception("Format " + name + " is not suitable for input", ErrorCodes::FORMAT_IS_NOT_SUITABLE_FOR_INPUT);
    }
    else
        throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}


static BlockOutputStreamPtr getOutputImpl(
    const String & name,
    WriteBuffer & buf,
    const Block & sample,
    const Context & context)
{
    const Settings & settings = context.getSettingsRef();
    FormatSettingsJSON json_settings(
        settings.output_format_json_quote_64bit_integers,
        settings.output_format_json_quote_denormals);

    if (name == "Native")
        return std::make_shared<NativeBlockOutputStream>(buf, 0, sample);
    else if (name == "RowBinary")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<BinaryRowOutputStream>(buf),
            sample);
    else if (name == "JSON")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<JSONRowOutputStream>(buf, sample, settings.output_format_write_statistics, json_settings),
            sample);
    else if (name == "JSONCompact")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<JSONCompactRowOutputStream>(
                buf,
                sample,
                settings.output_format_write_statistics,
                json_settings),
            sample);
    else if (name == "JSONEachRow")
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<JSONEachRowRowOutputStream>(buf, sample, json_settings),
            sample);
    else if (name == "Null")
        return std::make_shared<NullBlockOutputStream>(sample);
    else
        throw Exception("Unknown format " + name, ErrorCodes::UNKNOWN_FORMAT);
}

BlockOutputStreamPtr FormatFactory::getOutput(
    const String & name,
    WriteBuffer & buf,
    const Block & sample,
    const Context & context)
{
    /** Materialization is needed, because formats can use the functions `IDataType`,
      *  which only work with full columns.
      */
    return std::make_shared<MaterializingBlockOutputStream>(
        getOutputImpl(name, buf, materializeBlock(sample), context),
        sample);
}

} // namespace DB
