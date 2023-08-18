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

#include <Common/FmtUtils.h>
#include <Common/UnifiedLogPatternFormatter.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/Channel.h>
#include <Poco/Ext/ThreadNumber.h>
#include <fmt/core.h>
#include <sys/time.h>

#include <boost/algorithm/string.hpp>
#include <chrono>
#include <cstring>
#include <sstream>
#include <vector>

namespace DB
{
void UnifiedLogPatternFormatter::format(const Poco::Message & msg, std::string & text)
{
    DB::WriteBufferFromString wb(text);

    std::string timestamp_str = getTimestamp();

    auto prio = msg.getPriority();
    std::string prio_str = getPriorityString(prio);

    std::string source_str = "<unknown>";
    if (msg.getSourceFile())
        source_str = std::string(msg.getSourceFile()) + ":" + std::to_string(msg.getSourceLine());

    std::string message;
    const std::string & source = msg.getSource();
    if (!source.empty())
        message = fmt::format("{0}:{1}", source, msg.getText());
    else
        message = msg.getText();

    std::string thread_id_str = "thread_id=" + std::to_string(Poco::ThreadNumber::get());

    // std::vector<std::string> params{timestamp_str, prio_str, source_str, message, thread_id_str};

    DB::writeString("[", wb);
    DB::writeString(timestamp_str, wb);
    DB::writeString("] ", wb);

    DB::writeString("[", wb);
    DB::writeString(prio_str, wb);
    DB::writeString("] ", wb);

    DB::writeString("[", wb);
    DB::writeString(source_str, wb);
    DB::writeString("] ", wb);

    DB::writeString("[", wb);
    writeEscapedString(wb, message);
    DB::writeString("] ", wb);

    DB::writeString("[", wb);
    DB::writeString(thread_id_str, wb);
    DB::writeString("]", wb);
}

std::string UnifiedLogPatternFormatter::getPriorityString(const Poco::Message::Priority & priority)
{
    switch (priority)
    {
    case Poco::Message::Priority::PRIO_TRACE:
        return "TRACE";
    case Poco::Message::Priority::PRIO_DEBUG:
        return "DEBUG";
    case Poco::Message::Priority::PRIO_INFORMATION:
        return "INFO";
    case Poco::Message::Priority::PRIO_WARNING:
        return "WARN";
    case Poco::Message::Priority::PRIO_ERROR:
        return "ERROR";
    case Poco::Message::Priority::PRIO_FATAL:
        return "FATAL";
    case Poco::Message::Priority::PRIO_CRITICAL:
        return "CRITICAL";
    case Poco::Message::Priority::PRIO_NOTICE:
        return "NOTICE";

    default:
        return "UNKNOWN";
    }
}

std::string UnifiedLogPatternFormatter::getTimestamp()
{
    // The format is "yyyy/MM/dd HH:mm:ss.SSS ZZZZZ"
    auto time_point = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(time_point);

    std::tm buf_tm;
    std::tm * local_tm = localtime_r(&tt, &buf_tm);
    if (unlikely(!local_tm))
        return "1970/01/01 00:00:00.000 +00:00";
    int year = local_tm->tm_year + 1900;
    int month = local_tm->tm_mon + 1;
    int day = local_tm->tm_mday;
    int hour = local_tm->tm_hour;
    int minute = local_tm->tm_min;
    int second = local_tm->tm_sec;
    int milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch()).count() % 1000;

    int zone_offset = local_tm->tm_gmtoff;

    FmtBuffer fmt_buf;
    fmt_buf.fmtAppend("{0:04d}/{1:02d}/{2:02d} {3:02d}:{4:02d}:{5:02d}.{6:03d} ", year, month, day, hour, minute, second, milliseconds);

    // Handle time zone section
    int offset_value = std::abs(zone_offset);
    auto offset_seconds = std::chrono::seconds(offset_value);
    auto offset_tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>(offset_seconds);
    auto offset_tt = std::chrono::system_clock::to_time_t(offset_tp);
    std::tm * offset_tm = gmtime_r(&offset_tt, &buf_tm);
    if (unlikely(!offset_tm))
        return fmt_buf.toString() + "+00:00";
    if (zone_offset < 0)
        fmt_buf.append("-");
    else
        fmt_buf.append("+");

    fmt_buf.fmtAppend("{0:02d}:{1:02d}", offset_tm->tm_hour, offset_tm->tm_min);

    return fmt_buf.toString();
}

void UnifiedLogPatternFormatter::writeEscapedString(DB::WriteBuffer & wb, const std::string & str)
{
    if (!needJsonEncode(str))
    {
        DB::writeString(str, wb);
    }
    else
    {
        writeJSONString(wb, str);
    }
}

bool UnifiedLogPatternFormatter::needJsonEncode(const std::string & src)
{
    for (const uint8_t byte : src)
    {
        if (byte <= 0x20 || byte == 0x22 || byte == 0x3D || byte == 0x5B || byte == 0x5D)
            return true;
    }
    return false;
}

/// Copied from `IO/WriteHelpers.h`, without escaping `/`
void UnifiedLogPatternFormatter::writeJSONString(WriteBuffer & buf, const std::string & str)
{
    writeChar('"', buf);

    const char * begin = str.data();
    const char * end = str.data() + str.size();

    for (const char * it = begin; it != end; ++it)
    {
        switch (*it)
        {
        case '\b':
            writeChar('\\', buf);
            writeChar('b', buf);
            break;
        case '\f':
            writeChar('\\', buf);
            writeChar('f', buf);
            break;
        case '\n':
            writeChar('\\', buf);
            writeChar('n', buf);
            break;
        case '\r':
            writeChar('\\', buf);
            writeChar('r', buf);
            break;
        case '\t':
            writeChar('\\', buf);
            writeChar('t', buf);
            break;
        case '\\':
            writeChar('\\', buf);
            writeChar('\\', buf);
            break;
        case '"':
            writeChar('\\', buf);
            writeChar('"', buf);
            break;
        default:
            UInt8 c = *it;
            if (c <= 0x1F)
            {
                /// Escaping of ASCII control characters.

                UInt8 higher_half = c >> 4;
                UInt8 lower_half = c & 0xF;

                writeCString("\\u00", buf);
                writeChar('0' + higher_half, buf);

                if (lower_half <= 9)
                    writeChar('0' + lower_half, buf);
                else
                    writeChar('A' + lower_half - 10, buf);
            }
            else if (end - it >= 3 && it[0] == '\xE2' && it[1] == '\x80' && (it[2] == '\xA8' || it[2] == '\xA9'))
            {
                /// This is for compatibility with JavaScript, because unescaped line separators are prohibited in string literals,
                ///  and these code points are alternative line separators.

                if (it[2] == '\xA8')
                    writeCString("\\u2028", buf);
                if (it[2] == '\xA9')
                    writeCString("\\u2029", buf);

                /// Byte sequence is 3 bytes long. We have additional two bytes to skip.
                it += 2;
            }
            else
                writeChar(*it, buf);
        }
    }
    writeChar('"', buf);
}

} // namespace DB
