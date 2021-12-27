#pragma once

#include <Poco/Message.h>
#include <Poco/PatternFormatter.h>

#include <string>

namespace DB
{
class WriteBuffer;

/// https://github.com/tikv/rfcs/blob/ed764d7d014c420ee0cbcde99597020c4f75346d/text/0018-unified-log-format.md
class UnifiedLogPatternFormatter : public Poco::PatternFormatter
{
public:
    UnifiedLogPatternFormatter() = default;

    void format(const Poco::Message & msg, std::string & text) override;

private:
    static std::string getPriorityString(const Poco::Message::Priority & priority);

    static std::string getTimestamp();

    static bool needJsonEncode(const std::string & src);

    static void writeJSONString(DB::WriteBuffer & buf, const std::string & str);

    static void writeEscapedString(DB::WriteBuffer & wb, const std::string & str);
};

} // namespace DB
