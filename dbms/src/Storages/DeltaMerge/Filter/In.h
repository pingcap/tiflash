#pragma once

#include <Storages/DeltaMerge/Filter/RSOperator.h>

namespace DB
{
namespace DM
{
class In : public RSOperator
{
    Attr attr;
    Fields values;

public:
    In(const Attr & attr_, const Fields & values_)
        : attr(attr_)
        , values(values_)
    {
        if (unlikely(values.empty()))
            throw Exception("Unexpected empty values");
    }

    String name() override { return "in"; }

    Attrs getAttrs() override { return {attr}; }

    String toDebugString() override
    {
        String s = R"({"op":")" + name() + R"(","col":")" + attr.col_name + R"(","value":"[)";
        for (auto & v : values)
            s += "\"" + applyVisitor(FieldVisitorToDebugString(), v) + "\",";
        s.pop_back();
        return s + "]}";
    };


    RSResult roughCheck(size_t pack_id, const RSCheckParam & param) override
    {
        GET_RSINDEX_FROM_PARAM_NOT_FOUND_RETURN_SOME(param, attr, rsindex);
        // TODO optimize for IN
        RSResult res = rsindex.minmax->checkEqual(pack_id, values[0], rsindex.type);
        for (size_t i = 1; i < values.size(); ++i)
            res = res || rsindex.minmax->checkEqual(pack_id, values[i], rsindex.type);
        return res;
    }
};


} // namespace DM

} // namespace DB
