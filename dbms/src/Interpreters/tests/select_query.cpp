// Copyright 2022 PingCAP, Ltd.
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

#include <Databases/DatabaseOrdinary.h>
#include <Databases/IDatabase.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/loadMetadata.h>
#include <Poco/ConsoleChannel.h>
#include <Storages/StorageLog.h>
#include <Storages/System/attachSystemTables.h>
#include <common/DateLUT.h>

#include <iomanip>
#include <iostream>


using namespace DB;

int main(int, char **)
try
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");

    /// Pre-initialize the `DateLUT` so that the first initialization does not affect the measured execution speed.
    DateLUT::instance();

    Context context = Context::createGlobal();

    context.setPath("./");

    loadMetadata(context);

    DatabasePtr system = std::make_shared<DatabaseOrdinary>("system", "./metadata/system/", context);
    context.addDatabase("system", system);
    system->loadTables(context, nullptr, false);
    attachSystemTablesLocal(*context.getDatabase("system"));
    context.setCurrentDatabase("default");

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    WriteBufferFromFileDescriptor out(STDOUT_FILENO);

    executeQuery(in, out, /* allow_into_outfile = */ false, context, {});

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
