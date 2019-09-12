#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{

namespace DM
{

class Unsupported : public RSOperator
{
    String content;
    bool   is_not;

public:
    Unsupported(const String & content_) : content(content_), is_not(false) {}
    Unsupported(const String & content_, bool is_not_) : content(content_), is_not(is_not_) {}

    String name() override { return "unsupported"; }
    String toString() override
    {
        return R"({"op":")" + name() +     //
            R"(","content":")" + content + //
            R"(","is_not":")" + DB::toString(is_not) + "\"}";
    }

    RSResult roughCheck(const RSCheckParam & /*param*/) override { return Some; }

    RSOperatorPtr applyNot() override { return createUnsupported(content, !is_not); };
};

} // namespace DM

} // namespace DB