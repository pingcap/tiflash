// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/libs/libcommon/include/common/ErrorHandlers.h

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

#include <Common/Exception.h>
#include <Poco/ErrorHandler.h>
#include <common/logger_useful.h>


/** ErrorHandler for Poco::Thread,
  *  that in case of unhandled exception,
  *  logs exception message and terminates the process.
  */
class KillingErrorHandler : public Poco::ErrorHandler
{
public:
    void exception(const Poco::Exception &) override { std::terminate(); }
    void exception(const std::exception &) override { std::terminate(); }
    void exception() override { std::terminate(); }
};


/** Log exception message.
  */
class ServerErrorHandler : public Poco::ErrorHandler
{
public:
    void exception(const Poco::Exception &) override { logException(); }
    void exception(const std::exception &) override { logException(); }
    void exception() override { logException(); }

private:
    Poco::Logger * log = &Poco::Logger::get("ServerErrorHandler");

    void logException() { DB::tryLogCurrentException(log); }
};
