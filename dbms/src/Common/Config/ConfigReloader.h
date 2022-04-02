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

#pragma once

#include <time.h>

#include <condition_variable>
#include <functional>
#include <list>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "ConfigProcessor.h"


namespace Poco
{
class Logger;
}

namespace DB
{
class Context;

/** Every two seconds checks configuration files for update.
  * If configuration is changed, then config will be reloaded by ConfigProcessor
  *  and the reloaded config will be applied via Updater functor.
  * It doesn't take into account changes of --config-file, <users_config> and <include_from> parameters.
  */
class ConfigReloader
{
public:
    using Updater = std::function<void(ConfigurationPtr)>;

    /** include_from_path is usually /etc/metrika.xml (i.e. value of <include_from> tag)
      */
    ConfigReloader(const std::string & path, Updater && updater, bool already_loaded, const char * name = "CfgReloader");

    virtual ~ConfigReloader();

    /// Call this method to run the backround thread.
    virtual void start();

    /// Reload immediately. For SYSTEM RELOAD CONFIG query.
    void reload() { reloadIfNewer(/* force */ true, /* throw_on_error */ true); }

protected:
    void reloadIfNewer(bool force, bool throw_on_error);
    Updater & getUpdater() { return updater; }

private:
    void run();

    struct FileWithTimestamp;

    struct FilesChangesTracker
    {
        std::set<FileWithTimestamp> files;

        void addIfExists(const std::string & path);
        bool isDifferOrNewerThan(const FilesChangesTracker & rhs) const;
    };

    FilesChangesTracker getNewFileList() const;

protected:
    const char * name;

private:
    static constexpr auto reload_interval = std::chrono::seconds(2);

    Poco::Logger * log = &Poco::Logger::get(name);

    std::string path;
    FilesChangesTracker files;

    Updater updater;

    std::atomic<bool> quit{false};
    std::thread thread;

    /// Locked inside reloadIfNewer.
    std::mutex reload_mutex;
};

} // namespace DB
