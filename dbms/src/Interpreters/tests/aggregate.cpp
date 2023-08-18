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
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Interpreters/Aggregator.h>

#include <iomanip>
#include <iostream>


int main(int argc, char ** argv)
{
    using namespace DB;

    try
    {
        size_t n = argc == 2 ? atoi(argv[1]) : 10;

        Block block;

        {
            ColumnWithTypeAndName column;
            column.name = "x";
            column.type = std::make_shared<DataTypeInt16>();
            auto col = ColumnInt16::create();
            auto & vec_x = col->getData();

            vec_x.resize(n);
            for (size_t i = 0; i < n; ++i)
                vec_x[i] = i % 9;

            column.column = std::move(col);
            block.insert(column);
        }

        const char * strings[] = {"abc", "def", "abcd", "defg", "ac"};

        {
            ColumnWithTypeAndName column;
            column.name = "s1";
            column.type = std::make_shared<DataTypeString>();
            auto col = ColumnString::create();

            for (size_t i = 0; i < n; ++i)
                col->insert(std::string(strings[i % 5]));

            column.column = std::move(col);
            block.insert(column);
        }

        {
            ColumnWithTypeAndName column;
            column.name = "s2";
            column.type = std::make_shared<DataTypeString>();
            auto col = ColumnString::create();

            for (size_t i = 0; i < n; ++i)
                col->insert(std::string(strings[i % 3]));

            column.column = std::move(col);
            block.insert(column);
        }

        BlockInputStreamPtr stream = std::make_shared<OneBlockInputStream>(block);
        AggregatedDataVariants aggregated_data_variants;

        AggregateFunctionFactory factory;

        AggregateDescriptions aggregate_descriptions(1);

        DataTypes empty_list_of_types;
        aggregate_descriptions[0].function = factory.get("count", empty_list_of_types);

        Aggregator::Params params(stream->getHeader(), {0, 1}, aggregate_descriptions, false);

        Aggregator aggregator(params);

        {
            Stopwatch stopwatch;
            stopwatch.start();

            KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(false);
            FileProviderPtr file_provider = std::make_shared<FileProvider>(key_manager, false);
            aggregator.execute(stream, aggregated_data_variants, file_provider);

            stopwatch.stop();
            std::cout << std::fixed << std::setprecision(2)
                      << "Elapsed " << stopwatch.elapsedSeconds() << " sec."
                      << ", " << n / stopwatch.elapsedSeconds() << " rows/sec."
                      << std::endl;
        }
    }
    catch (const Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }

    return 0;
}