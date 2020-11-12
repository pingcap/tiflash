#pragma once

#include <Poco/Message.h>
#include <Poco/PatternFormatter.h>

#include <string>

namespace DB
{

class WriteBuffer;

class UnifiedLogPatternFormatter : public Poco::PatternFormatter
{
public:
    UnifiedLogPatternFormatter() {}

    void format(const Poco::Message & msg, std::string & text) override;

private:
    std::string getPriorityString(const Poco::Message::Priority & priority) const;

    std::string getTimestamp() const;

    bool needJsonEncode(const std::string & src);

    void writeJSONString(DB::WriteBuffer & wb, const std::string & str);

    void writeEscapedString(DB::WriteBuffer & wb, const std::string & str);
};

} // namespace DB
