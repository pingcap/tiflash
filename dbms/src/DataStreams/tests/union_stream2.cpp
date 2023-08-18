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

#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/LimitBlockInputStream.h>
#include <DataStreams/UnionBlockInputStream.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Interpreters/Context.h>
#include <Interpreters/loadMetadata.h>
#include <Storages/RegionQueryInfo.h>
#include <Storages/System/StorageSystemNumbers.h>

#include <iomanip>
#include <iostream>


using namespace DB;

int main(int, char **)
try
{
    Context context = Context::createGlobal();
    Settings settings = context.getSettings();

    context.setPath("./");

    loadMetadata(context);

    Names column_names;
    column_names.push_back("WatchID");

    StoragePtr table = context.getTable("default", "hits6");

    QueryProcessingStage::Enum stage;
    BlockInputStreams streams = table->read(column_names, {}, context, stage, settings.max_block_size, settings.max_threads);

    for (size_t i = 0, size = streams.size(); i < size; ++i)
        streams[i] = std::make_shared<AsynchronousBlockInputStream>(streams[i]);

    BlockInputStreamPtr stream = std::make_shared<UnionBlockInputStream<>>(streams, BlockInputStreams{}, settings.max_threads, /*req_id=*/"");
    stream = std::make_shared<LimitBlockInputStream>(stream, 10, 0, "");

    WriteBufferFromFileDescriptor wb(STDERR_FILENO);
    Block sample = table->getSampleBlock();
    BlockOutputStreamPtr out = context.getOutputFormat("TabSeparated", wb, sample);

    copyData(*stream, *out);

    return 0;
}
catch (const Exception & e)
{
    std::cerr << e.what() << ", " << e.displayText() << std::endl
              << std::endl
              << "Stack trace:" << std::endl
              << e.getStackTrace().toString();
    return 1;
}
