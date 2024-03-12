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

#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/Exception.h>
#include <Common/setThreadName.h>
#include <Poco/File.h>
#include <Poco/Util/Application.h>
#include <common/logger_useful.h>


namespace DB
{
constexpr decltype(ConfigReloader::reload_interval) ConfigReloader::reload_interval;

ConfigReloader::ConfigReloader(const std::string & path_, Updater && updater_, bool already_loaded, const char * name_)
    : name(name_)
    , path(path_)
    , updater(std::move(updater_))
{
    if (!already_loaded)
        reloadIfNewer(/* force = */ true, /* throw_on_error = */ true);
}


void ConfigReloader::start()
{
    thread = std::thread(&ConfigReloader::run, this);
}

ConfigReloader::~ConfigReloader()
{
    try
    {
        quit = true;

        cv.notify_all();
        if (thread.joinable())
            thread.join();
    }
    catch (...)
    {
        DB::tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void ConfigReloader::run()
{
    setThreadName(name);

    while (true)
    {
        std::unique_lock lock(reload_mutex);
        if (cv.wait_for(lock, reload_interval, [this]() { return quit.load(); }))
            return;

        reloadIfNewer(false, /* throw_on_error = */ false);
    }
}

void ConfigReloader::reloadIfNewer(bool force, bool throw_on_error)
{
    FilesChangesTracker new_files = getNewFileList();
    bool config_object_updated = false;
    for (const auto & conf : config_objects)
    {
        if (conf->fileUpdated())
        {
            config_object_updated = true;
            break;
        }
    }

    if (force || (new_files.valid() && new_files.isDifferOrNewerThan(files)) || config_object_updated)
    {
        ConfigProcessor config_processor(path);
        ConfigProcessor::LoadedConfig loaded_config;
        try
        {
            LOG_DEBUG(log, "Loading config from `{}`", path);
            loaded_config = config_processor.loadConfig();
        }
        catch (...)
        {
            if (throw_on_error)
                throw;

            tryLogCurrentException(log, fmt::format("Error loading config from `{}`", path));
            return;
        }

        /** We should remember last modification time if and only if config was sucessfully loaded
         * Otherwise a race condition could occur during config files update:
         *  File is contain raw (and non-valid) data, therefore config is not applied.
         *  When file has been written (and contain valid data), we don't load new data since modification time remains the same.
         */
        if (!loaded_config.loaded_from_preprocessed)
            files = std::move(new_files);

        try
        {
            updater(loaded_config.configuration);
        }
        catch (...)
        {
            if (throw_on_error)
                throw;
            tryLogCurrentException(log, fmt::format("Error updating configuration from `{}` config.", path));
        }
    }
}

FilesChangesTracker ConfigReloader::getNewFileList() const
{
    FilesChangesTracker file_list;

    file_list.addIfExists(path);

    return file_list;
}

} // namespace DB
