#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{

namespace DM
{

class Unsupported : public RSOperator
{
    String content;
    String reason;
    bool   is_not;

public:
    Unsupported(const String & content_, const String & reason_) : Unsupported(content_, reason_, false) {}
    Unsupported(const String & content_, const String & reason_, bool is_not_) : content(content_), reason(reason_), is_not(is_not_) {}

    String name() override { return "unsupported"; }

    Attrs getAttrs() override { return {}; }

    String toDebugString() override
    {
        return R"({"op":")" + name() +     //
            R"(","reason":")" + reason +   //
            R"(","content":")" + content + //
            R"(","is_not":")" + DB::toString(is_not) + "\"}";
    }

    RSResult roughCheck(size_t /*pack_id*/, const RSCheckParam & /*param*/) override { return Some; }

    RSOperatorPtr applyNot() override { return createUnsupported(content, reason, !is_not); };
};

} // namespace DM

} // namespace DB
