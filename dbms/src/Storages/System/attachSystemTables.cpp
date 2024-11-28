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

#include <Databases/IDatabase.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>
#include <Storages/System/StorageSystemBuildOptions.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/StorageSystemDTLocalIndexes.h>
#include <Storages/System/StorageSystemDTSegments.h>
#include <Storages/System/StorageSystemDTTables.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/System/StorageSystemEvents.h>
#include <Storages/System/StorageSystemFunctions.h>
#include <Storages/System/StorageSystemGraphite.h>
#include <Storages/System/StorageSystemMacros.h>
#include <Storages/System/StorageSystemMetrics.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/StorageSystemProcesses.h>
#include <Storages/System/StorageSystemSettings.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/System/attachSystemTables.h>


namespace DB
{
void attachSystemTablesLocal(IDatabase & system_database)
{
    system_database.attachTable("one", StorageSystemOne::create("one"));
    system_database.attachTable("numbers", StorageSystemNumbers::create("numbers", false));
    system_database.attachTable("numbers_mt", StorageSystemNumbers::create("numbers_mt", true));
    system_database.attachTable("databases", StorageSystemDatabases::create("databases"));
    system_database.attachTable("dt_tables", StorageSystemDTTables::create("dt_tables"));
    system_database.attachTable("dt_segments", StorageSystemDTSegments::create("dt_segments"));
    system_database.attachTable("dt_local_indexes", StorageSystemDTLocalIndexes::create("dt_local_indexes"));
    system_database.attachTable("tables", StorageSystemTables::create("tables"));
    system_database.attachTable("columns", StorageSystemColumns::create("columns"));
    system_database.attachTable("functions", StorageSystemFunctions::create("functions"));
    system_database.attachTable("events", StorageSystemEvents::create("events"));
    system_database.attachTable("settings", StorageSystemSettings::create("settings"));
    system_database.attachTable("build_options", StorageSystemBuildOptions::create("build_options"));
}

void attachSystemTablesServer(IDatabase & system_database)
{
    attachSystemTablesLocal(system_database);
    system_database.attachTable("processes", StorageSystemProcesses::create("processes"));
    system_database.attachTable("metrics", StorageSystemMetrics::create("metrics"));
    system_database.attachTable("graphite_retentions", StorageSystemGraphite::create("graphite_retentions"));
    system_database.attachTable("macros", StorageSystemMacros::create("macros"));
}

void attachSystemTablesAsync(IDatabase & system_database, AsynchronousMetrics & async_metrics)
{
    system_database.attachTable(
        "asynchronous_metrics",
        StorageSystemAsynchronousMetrics::create("asynchronous_metrics", async_metrics));
}

} // namespace DB
