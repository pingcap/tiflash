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

#include <Poco/Net/SocketStream.h>
#include <Poco/Net/StreamSocket.h>
#include <Poco/Util/Application.h>
#include <common/logger_useful.h>
#include <time.h>

#include <string>


/// Writes to Graphite in the following format
/// path value timestamp\n
/// path can be arbitrary nested. Elements are separated by '.'
/// Example: root_path.server_name.sub_path.key
class GraphiteWriter
{
public:
    explicit GraphiteWriter(const std::string & config_name, const std::string & sub_path = "");

    template <typename T>
    using KeyValuePair = std::pair<std::string, T>;
    template <typename T>
    using KeyValueVector = std::vector<KeyValuePair<T>>;

    template <typename T>
    void write(
        const std::string & key,
        const T & value,
        time_t timestamp = 0,
        const std::string & custom_root_path = "")
    {
        writeImpl(KeyValuePair<T>{key, value}, timestamp, custom_root_path);
    }

    template <typename T>
    void write(const KeyValueVector<T> & key_val_vec, time_t timestamp = 0, const std::string & custom_root_path = "")
    {
        writeImpl(key_val_vec, timestamp, custom_root_path);
    }

    static std::string getPerServerPath(const std::string & server_name, const std::string & root_path = "one_min");

private:
    template <typename T>
    void writeImpl(const T & data, time_t timestamp, const std::string & custom_root_path)
    {
        if (!timestamp)
            timestamp = time(nullptr);

        try
        {
            Poco::Net::SocketAddress socket_address(host, port);
            Poco::Net::StreamSocket socket(socket_address);
            socket.setSendTimeout(Poco::Timespan(timeout * 1000000));
            Poco::Net::SocketStream str(socket);

            out(str, data, timestamp, custom_root_path);
        }
        catch (const Poco::Exception & e)
        {
            LOG_WARNING(
                &Poco::Util::Application::instance().logger(),
                "Fail to write to Graphite {}:{}. e.what() = {}, e.message() = {}",
                host,
                port,
                e.what(),
                e.message());
        }
    }

    template <typename T>
    void out(std::ostream & os, const KeyValuePair<T> & key_val, time_t timestamp, const std::string & custom_root_path)
    {
        os << (custom_root_path.empty() ? root_path : custom_root_path) << '.' << key_val.first << ' ' << key_val.second
           << ' ' << timestamp << '\n';
    }

    template <typename T>
    void out(
        std::ostream & os,
        const KeyValueVector<T> & key_val_vec,
        time_t timestamp,
        const std::string & custom_root_path)
    {
        for (const auto & key_val : key_val_vec)
            out(os, key_val, timestamp, custom_root_path);
    }

    std::string root_path;

    int port;
    std::string host;
    double timeout;
};
