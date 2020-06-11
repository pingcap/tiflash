#include <chrono>
#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Poco/Channel.h>
#include <Poco/Ext/ThreadNumber.h>
#include <Poco/PatternFormatter.h>
#include <sys/time.h>
#include <boost/algorithm/string.hpp>


namespace DB
{

using Poco::Message;

class UnifiedLogPatternFormatter : public Poco::PatternFormatter
{
public:
    UnifiedLogPatternFormatter() {}

    void format(const Poco::Message & msg, std::string & text) override;

private:
    std::string getPriorityString(const Poco::Message::Priority & priority) const;

    std::string getTimestamp() const;
};

void UnifiedLogPatternFormatter::format(const Poco::Message & msg, std::string & text)
{
    DB::WriteBufferFromString wb(text);

    std::string timestamp_str = getTimestamp();

    auto prio = msg.getPriority();
    std::string prio_str = getPriorityString(prio);

    std::string source_str = "<unknown>";
    if (msg.getSourceFile())
        source_str = "<" + std::string(msg.getSourceFile()) + ":" + std::to_string(msg.getSourceLine()) + ">";

    std::string message = msg.getText();
    boost::replace_all(message, "\n", "\\n");
    boost::replace_all(message, "\"", "\\\"");
    message = "\"" + message + "\"";

    std::string thread_id_str = "thread_id=" + std::to_string(Poco::ThreadNumber::get());

    std::vector<std::string> params{timestamp_str, prio_str, source_str, message, thread_id_str};

    for (size_t i = 0; i < params.size(); i++)
    {
        DB::writeString("[", wb);
        DB::writeString(params[i], wb);
        DB::writeString("] ", wb);
    }
}

std::string UnifiedLogPatternFormatter::getPriorityString(const Poco::Message::Priority & priority) const
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

std::string UnifiedLogPatternFormatter::getTimestamp() const
{
    auto time_point = std::chrono::system_clock::now();
    auto tt = std::chrono::system_clock::to_time_t(time_point);

    std::tm * local_tm = std::localtime(&tt);
    int year = local_tm->tm_year + 1900;
    int month = local_tm->tm_mon + 1;
    int day = local_tm->tm_mday;
    int hour = local_tm->tm_hour;
    int minute = local_tm->tm_min;
    int second = local_tm->tm_sec;
    int milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch()).count() % 1000000 / 1000;

    int zone_offset = local_tm->tm_gmtoff;

    char buffer[] = "yyyy/MM/dd HH:mm:ss.SSS";

    std::sprintf(buffer, "%04d/%02d/%02d %02d:%02d:%02d.%03d", year, month, day, hour, minute, second, milliseconds);

    std::stringstream ss;
    ss << buffer << " ";

    // Handle time zone section
    int offset_value = std::abs(zone_offset);
    auto offset_seconds = std::chrono::seconds(offset_value);
    auto offset_tp = std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds>(offset_seconds);
    auto offset_tt = std::chrono::system_clock::to_time_t(offset_tp);
    std::tm * offset_tm = std::gmtime(&offset_tt);
    if (zone_offset < 0)
        ss << "-";
    else
        ss << "+";
    char buff[] = "hh:mm";
    std::sprintf(buff, "%02d:%02d", offset_tm->tm_hour, offset_tm->tm_min);

    ss << buff;

    std::string result = ss.str();
    return result;
}

} // namespace DB