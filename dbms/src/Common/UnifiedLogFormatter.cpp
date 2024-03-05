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
#include <Common/UnifiedLogFormatter.h>
#include <IO/Buffer/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/Ext/ThreadNumber.h>
#include <fmt/compile.h>
#include <sys/time.h>

#include <chrono>
#include <vector>

#define RESET_FONT "\033[0m"
#define BOLD_FONT "\033[1m"
#define BLACK "\033[30m"
#define RED "\033[31m"
#define GREEN "\033[32m"
#define YELLOW "\033[33m"
#define BLUE "\033[34m"
#define MAGENTA "\033[35m"
#define CYAN "\033[36m"
#define WHITE "\033[37m"
#define BRIGHT_BLACK "\033[90m"
#define BRIGHT_RED "\033[91m"
#define BRIGHT_GREEN "\033[92m"
#define BRIGHT_YELLOW "\033[93m"
#define BRIGHT_BLUE "\033[94m"
#define BRIGHT_MAGENTA "\033[95m"
#define BRIGHT_CYAN "\033[96m"
#define BRIGHT_WHITE "\033[97m"

namespace DB
{

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

void writePriorityColor(FmtBuffer & buf, const Poco::Message::Priority & priority)
{
    switch (priority)
    {
    case Poco::Message::Priority::PRIO_TRACE:
        buf.append(BRIGHT_BLACK);
        break;
    case Poco::Message::Priority::PRIO_DEBUG:
        buf.append(BRIGHT_BLACK);
        break;
    case Poco::Message::Priority::PRIO_INFORMATION:
        buf.append(BRIGHT_BLUE);
        break;
    case Poco::Message::Priority::PRIO_WARNING:
        buf.append(YELLOW);
        break;
    case Poco::Message::Priority::PRIO_ERROR:
        buf.append(RED);
        break;
    case Poco::Message::Priority::PRIO_FATAL:
        buf.append(RED);
        break;
    case Poco::Message::Priority::PRIO_CRITICAL:
        buf.append(RED);
        break;
    case Poco::Message::Priority::PRIO_NOTICE:
        break;
    default:
        break;
    }
}

void writeMessageColor(FmtBuffer & buf, const Poco::Message::Priority & priority)
{
    switch (priority)
    {
    case Poco::Message::Priority::PRIO_WARNING:
        buf.append(YELLOW);
        break;
    case Poco::Message::Priority::PRIO_ERROR:
        buf.append(RED);
        break;
    case Poco::Message::Priority::PRIO_FATAL:
        buf.append(RED);
        break;
    case Poco::Message::Priority::PRIO_CRITICAL:
        buf.append(RED);
        break;
    default:
        break;
    }
}

void writePriority(FmtBuffer & buf, const Poco::Message::Priority & priority)
{
    switch (priority)
    {
    case Poco::Message::Priority::PRIO_TRACE:
        buf.append("TRACE");
        break;
    case Poco::Message::Priority::PRIO_DEBUG:
        buf.append("DEBUG");
        break;
    case Poco::Message::Priority::PRIO_INFORMATION:
        buf.append("INFO");
        break;
    case Poco::Message::Priority::PRIO_WARNING:
        buf.append("WARN");
        break;
    case Poco::Message::Priority::PRIO_ERROR:
        buf.append("ERROR");
        break;
    case Poco::Message::Priority::PRIO_FATAL:
        buf.append("FATAL");
        break;
    case Poco::Message::Priority::PRIO_CRITICAL:
        buf.append("CRITICAL");
        break;
    case Poco::Message::Priority::PRIO_NOTICE:
        buf.append("NOTICE");
        break;
    default:
        buf.append("UNKNOWN");
        break;
    }
}

void writeTimestamp(FmtBuffer & buf)
{
    // The format is "yyyy/MM/dd HH:mm:ss.SSS ZZZZZ"
    auto time_point = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(time_point);

    std::tm buf_tm{};
    std::tm * local_tm = localtime_r(&tt, &buf_tm);
    if (unlikely(!local_tm))
    {
        buf.append("1970/01/01 00:00:00.000 +00:00");
        return;
    }

    int year = local_tm->tm_year + 1900;
    int month = local_tm->tm_mon + 1;
    int day = local_tm->tm_mday;
    int hour = local_tm->tm_hour;
    int minute = local_tm->tm_min;
    int second = local_tm->tm_sec;
    int milliseconds
        = std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch()).count() % 1000;

    int zone_offset = local_tm->tm_gmtoff;

    buf.fmtAppend(
        FMT_COMPILE("{0:04d}/{1:02d}/{2:02d} {3:02d}:{4:02d}:{5:02d}.{6:03d} "),
        year,
        month,
        day,
        hour,
        minute,
        second,
        milliseconds);

    // Handle time zone section
    int offset_value = std::abs(zone_offset);
    auto offset_seconds = std::chrono::seconds(offset_value);
    auto offset_tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>(offset_seconds);
    auto offset_tt = std::chrono::system_clock::to_time_t(offset_tp);
    std::tm * offset_tm = gmtime_r(&offset_tt, &buf_tm);
    if (unlikely(!offset_tm))
    {
        buf.append("+00:00");
        return;
    }

    if (zone_offset < 0)
        buf.append('-');
    else
        buf.append('+');

    buf.fmtAppend(FMT_COMPILE("{0:02d}:{1:02d}"), offset_tm->tm_hour, offset_tm->tm_min);
}

JsonEncodeKind needJsonEncode(const std::string & src)
{
    bool needs_quote = false;
    bool json_encode_cannot_copy = false;

    for (const uint8_t byte : src)
    {
        if (unlikely(byte <= 0x20 || byte == 0x22 || byte == 0x3D || byte == 0x5B || byte == 0x5D))
            // See https://github.com/tikv/rfcs/blob/master/text/0018-unified-log-format.md#log-fields-section
            needs_quote = true;
        // NOLINTNEXTLINE
        if (unlikely(byte <= 0x1F || byte == '\n' || byte == '\r' || byte == '\t' || byte == '\\' || byte == '"'))
            json_encode_cannot_copy = true;
        if (unlikely(needs_quote && json_encode_cannot_copy))
            return JsonEncodeKind::Encode;
    }
    if (needs_quote)
        return JsonEncodeKind::AddQuoteAndCopy;
    else
        return JsonEncodeKind::DirectCopy;
}

void writeJSONString(FmtBuffer & buf, const std::string & str)
{
    buf.append('"');

    for (const uint8_t byte : str)
    {
        switch (byte)
        {
        case '\n':
            buf.append("\\n");
            break;
        case '\r':
            buf.append("\\r");
            break;
        case '\t':
            buf.append("\\t");
            break;
        case '\\':
            buf.append("\\\\");
            break;
        case '"':
            buf.append("\\\"");
            break;
        default:
            if (unlikely(byte <= 0x1F))
                buf.fmtAppend(FMT_COMPILE("\\u{:04x}"), byte);
            else
                buf.append(byte);
        }
    }

    buf.append('"');
}

void writeEscapedString(FmtBuffer & buf, const std::string & str)
{
    auto encode_kind = needJsonEncode(str);
    switch (encode_kind)
    {
    case JsonEncodeKind::DirectCopy:
        buf.append(str);
        break;
    case JsonEncodeKind::AddQuoteAndCopy:
        buf.append('"');
        buf.append(str);
        buf.append('"');
        break;
    case JsonEncodeKind::Encode:
        writeJSONString(buf, str);
        break;
    }
}

template <bool enable_color>
void UnifiedLogFormatter<enable_color>::format(const Poco::Message & msg, std::string & text)
{
    FmtBuffer buf;

    // Timestamp
    {
        if constexpr (enable_color)
            buf.append(BRIGHT_GREEN);

        buf.append('[');
        writeTimestamp(buf);
        buf.append("] ");

        if constexpr (enable_color)
            buf.append(RESET_FONT);
    }

    // Priority
    {
        if constexpr (enable_color)
        {
            buf.append(BOLD_FONT);
            writePriorityColor(buf, msg.getPriority());
        }

        buf.append('[');
        writePriority(buf, msg.getPriority());
        buf.append("] ");

        if constexpr (enable_color)
            buf.append(RESET_FONT);
    }

    // Source File
    {
        if constexpr (enable_color)
            buf.append(MAGENTA);

        if (unlikely(!msg.getSourceFile()))
            buf.append("[<unknown>] ");
        else
            buf.fmtAppend(FMT_COMPILE("[{}:{}] "), msg.getSourceFile(), msg.getSourceLine());

        if constexpr (enable_color)
            buf.append(RESET_FONT);
    }
    // Message
    {
        if constexpr (enable_color)
            writeMessageColor(buf, msg.getPriority());

        buf.append('[');
        writeEscapedString(buf, msg.getText());
        buf.append("] ");

        if constexpr (enable_color)
            buf.append(RESET_FONT);
    }

    // Source and Identifiers
    if constexpr (enable_color)
        buf.append(BRIGHT_BLACK);

    {
        const std::string & source = msg.getSource();
        if (!source.empty())
        {
            buf.append("[source=");
            writeEscapedString(buf, source);
            buf.append("] ");
        }
    }
    // Thread ID
    {
        buf.fmtAppend(FMT_COMPILE("[thread_id={}]"), Poco::ThreadNumber::get());
    }

    if constexpr (enable_color)
        buf.append(RESET_FONT);

    text = buf.toString();
}

template class UnifiedLogFormatter<true>;
template class UnifiedLogFormatter<false>;

} // namespace DB
