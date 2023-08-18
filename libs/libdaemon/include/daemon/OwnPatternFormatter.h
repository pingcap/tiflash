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


#include <Poco/PatternFormatter.h>


/** Format log messages own way.
  * We can't obtain some details using Poco::PatternFormatter.
  *
  * Firstly, the thread number here is peaked not from Poco::Thread
  * threads only, but from all threads with number assigned (see ThreadNumber.h)
  *
  * Secondly, the local date and time are correctly displayed.
  * Poco::PatternFormatter does not work well with local time,
  * when timestamps are close to DST timeshift moments.
  * - see Poco sources and http://thread.gmane.org/gmane.comp.time.tz/8883
  *
  * Also it's made a bit more efficient (unimportant).
  */

class BaseDaemon;

class OwnPatternFormatter : public Poco::PatternFormatter
{
public:
    /// ADD_LAYER_TAG is needed only for Yandex.Metrika, that share part of ClickHouse code.
    enum Options
    {
        ADD_NOTHING = 0,
        ADD_LAYER_TAG = 1 << 0
    };

    explicit OwnPatternFormatter(const BaseDaemon * daemon_, Options options_ = ADD_NOTHING)
        : Poco::PatternFormatter("")
        , daemon(daemon_)
        , options(options_)
    {}

    void format(const Poco::Message & msg, std::string & text) override;

private:
    const BaseDaemon * daemon;
    Options options;
};
