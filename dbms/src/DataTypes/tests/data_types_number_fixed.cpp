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
#include <Common/Stopwatch.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromOStream.h>

#include <fstream>
#include <iostream>


int main(int, char **)
{
    using namespace DB;

    auto column = ColumnUInt64::create();
    ColumnUInt64::Container & vec = column->getData();
    DataTypeUInt64 data_type;

    Stopwatch stopwatch;
    size_t n = 10000000;

    vec.resize(n);
    for (size_t i = 0; i < n; ++i)
        vec[i] = i;

    std::ofstream ostr("test");
    WriteBufferFromOStream out_buf(ostr);

    stopwatch.restart();
    data_type.serializeBinaryBulkWithMultipleStreams(*column, [&](const IDataType::SubstreamPath &) { return &out_buf; }, 0, 0, true, {});
    stopwatch.stop();

    std::cout << "Elapsed: " << stopwatch.elapsedSeconds() << std::endl;

    return 0;
}
