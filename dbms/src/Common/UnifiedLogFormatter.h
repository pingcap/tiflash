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

#include <Common/FmtUtils.h>
#include <Poco/Formatter.h>
#include <Poco/Message.h>

#include <string>

namespace DB
{
class WriteBuffer;

/// https://github.com/tikv/rfcs/blob/ed764d7d014c420ee0cbcde99597020c4f75346d/text/0018-unified-log-format.md
class UnifiedLogFormatter : public Poco::Formatter
{
public:
    UnifiedLogFormatter() = default;

    void format(const Poco::Message & msg, std::string & text) override;

private:
    enum class JsonEncodeKind
    {
        /**
         * No need to encode, just copy the text
         */
        DirectCopy,

        /**
         * Add double quotes around the text is sufficient
         */
        AddQuoteAndCopy,

        /**
         * Need full JSON string encode
         */
        Encode,
    };

    static void writePriority(FmtBuffer & buf, const Poco::Message::Priority & priority);

    static void writeTimestamp(FmtBuffer & buf);

    static JsonEncodeKind needJsonEncode(const std::string & src);

    static void writeJSONString(FmtBuffer & buf, const std::string & str);

    static void writeEscapedString(FmtBuffer & buf, const std::string & str);
};

} // namespace DB
