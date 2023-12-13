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

#pragma once

#include <Common/Config/ConfigProcessor.h>
#include <Poco/FileChannel.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/NumberFormatter.h>
#include <Poco/Process.h>
#include <Poco/SyslogChannel.h>
#include <Poco/TaskNotification.h>
#include <Poco/ThreadPool.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/ServerApplication.h>
#include <Poco/Version.h>
#include <common/logger_useful.h>
#include <common/types.h>
#include <daemon/GraphiteWriter.h>
#include <port/unistd.h>
#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>

/// \brief Base class for applications that can run as daemons.
///
/// \code
/// # Some possible command line options:
/// #    --config-file, -C or --config - path to configuration file. By default - config.xml in the current directory.
/// #    --pid-file - PID file name. Default is pid
/// #    --log-file
/// #    --errorlog-file
/// #    --daemon - run as daemon; without this option, the program will be attached to the terminal and also output logs to stderr.
/// <daemon_name> --daemon --config-file=localfile.xml --log-file=log.log --errorlog-file=error.log
/// \endcode
///
/// You can configure different log options for different loggers used inside program
///  by providing subsections to "logger" in configuration file.
class BaseDaemon : public Poco::Util::ServerApplication
{
    friend class SignalListener;

public:
    static constexpr char DEFAULT_GRAPHITE_CONFIG_NAME[] = "graphite";

    BaseDaemon();
    ~BaseDaemon() override;

    /// Load configuration, prepare loggers, etc.
    void initialize(Poco::Util::Application &) override;

    /// Reads the configuration
    void reloadConfiguration();

    /// Builds the necessary loggers
    void buildLoggers(Poco::Util::AbstractConfiguration & config);

    /// Process command line parameters
    void defineOptions(Poco::Util::OptionSet & _options) override;

    /// Graceful shutdown
    static void terminate();

    /// Forceful shutdown
    void kill();

    /// Cancellation request has been received.
    bool isCancelled() const { return is_cancelled; }

    static BaseDaemon & instance() { return dynamic_cast<BaseDaemon &>(Poco::Util::Application::instance()); }

    /// return none if daemon doesn't exist, reference to the daemon otherwise
    static std::optional<std::reference_wrapper<BaseDaemon>> tryGetInstance() { return tryGetInstance<BaseDaemon>(); }

    /// Sleeps for the set number of seconds or until wakeup event
    void sleep(double seconds);

    void wakeup();

    /// Close the log files. The next time you write, new files will be created.
    void closeLogs();

    /// In Graphite, path(folder) components are separated by a dot.
    /// We have adopted the format root_path.hostname_yandex_ru.key
    /// root_path is by default one_min
    /// key - better to group by meaning. For example "meminfo.cached" or "meminfo.free", "meminfo.total"
    template <class T>
    void writeToGraphite(
        const std::string & key,
        const T & value,
        const std::string & config_name = DEFAULT_GRAPHITE_CONFIG_NAME,
        time_t timestamp = 0,
        const std::string & custom_root_path = "")
    {
        auto * writer = getGraphiteWriter(config_name);
        if (writer)
            writer->write(key, value, timestamp, custom_root_path);
    }

    template <class T>
    void writeToGraphite(
        const GraphiteWriter::KeyValueVector<T> & key_vals,
        const std::string & config_name = DEFAULT_GRAPHITE_CONFIG_NAME,
        time_t timestamp = 0,
        const std::string & custom_root_path = "")
    {
        auto * writer = getGraphiteWriter(config_name);
        if (writer)
            writer->write(key_vals, timestamp, custom_root_path);
    }

    template <class T>
    void writeToGraphite(
        const GraphiteWriter::KeyValueVector<T> & key_vals,
        const std::chrono::system_clock::time_point & current_time,
        const std::string & custom_root_path)
    {
        auto * writer = getGraphiteWriter();
        if (writer)
            writer->write(key_vals, std::chrono::system_clock::to_time_t(current_time), custom_root_path);
    }

    GraphiteWriter * getGraphiteWriter(const std::string & config_name = DEFAULT_GRAPHITE_CONFIG_NAME)
    {
        if (graphite_writers.count(config_name))
            return graphite_writers[config_name].get();
        return nullptr;
    }

    std::optional<size_t> getLayer() const { return layer; }

protected:
    virtual void logVersion() const;

    /// Used when exitOnTaskError()
    void handleNotification(Poco::TaskFailedNotification *);

    /// thread safe
    virtual void handleSignal(int signal_id);

    /// implementation of pipe termination signal handling does not require blocking the signal with sigprocmask in all threads
    void waitForTerminationRequest()
#if POCO_CLICKHOUSE_PATCH || POCO_VERSION >= 0x02000000 // in old upstream poco not virtual
        override
#endif
        ;
    /// thread safe
    virtual void onInterruptSignals(int signal_id);

    template <class Daemon>
    static std::optional<std::reference_wrapper<Daemon>> tryGetInstance();

    virtual std::string getDefaultCorePath() const;

    /// Creation and automatic deletion of a pid file.
    struct PID
    {
        std::string file;

        /// Create an object without creating a PID file
        PID() = default;

        /// Create object, create PID file
        explicit PID(const std::string & file_) { seed(file_); }

        /// Create a PID file
        void seed(const std::string & file_);

        /// Delete PID file
        void clear();

        ~PID() { clear(); }
    };

    PID pid;

    std::atomic_bool is_cancelled{false};

    /// The flag is set by a message from Task (in case of an emergency stop).
    bool task_failed = false;

    bool log_to_console = false;

    /// An event to wake up to while waiting
    Poco::Event wakeup_event;

    /// The stream in which the HUP/USR1 signal is received to close the logs.
    Poco::Thread signal_listener_thread;
    std::unique_ptr<Poco::Runnable> signal_listener;

    /// Log files.
    Poco::AutoPtr<Poco::FileChannel> log_file;
    Poco::AutoPtr<Poco::FileChannel> error_log_file;
    Poco::AutoPtr<Poco::FileChannel> tracing_log_file;
    Poco::AutoPtr<Poco::SyslogChannel> syslog_channel;

    std::map<std::string, std::unique_ptr<GraphiteWriter>> graphite_writers;

    std::optional<size_t> layer;

    std::mutex signal_handler_mutex;
    std::condition_variable signal_event;
    std::atomic_size_t terminate_signals_counter{0};
    std::atomic_size_t sigint_signals_counter{0};

    std::string config_path;
    ConfigProcessor::LoadedConfig loaded_config;
    Poco::Util::AbstractConfiguration * last_configuration = nullptr;

private:
    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::string config_logger;
};


template <class Daemon>
std::optional<std::reference_wrapper<Daemon>> BaseDaemon::tryGetInstance()
{
    Daemon * ptr = nullptr;
    try
    {
        ptr = dynamic_cast<Daemon *>(&Poco::Util::Application::instance());
    }
    catch (const Poco::NullPointerException &)
    {
        /// if daemon doesn't exist than instance() throw NullPointerException
    }

    if (ptr)
        return std::optional<std::reference_wrapper<Daemon>>(*ptr);
    else
        return {};
}
