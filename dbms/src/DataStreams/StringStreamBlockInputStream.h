#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataStreams/dedupUtils.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{

class StringStreamBlockInputStream : public IProfilingBlockInputStream
{
private:
    Block readImpl() override
    {
        Block block;
        if (data.empty())
            return block;

        auto col = ColumnString::create();
        for (const auto & str : data)
            col->insert(Field(str));
        data.clear();
        block.insert({std::move(col), std::make_shared<DataTypeString>(), col_name});
        return block;
    }

    Block getHeader() const override
    {
        Block block;
        auto col = ColumnString::create();
        block.insert({std::move(col), std::make_shared<DataTypeString>(), col_name});
        return block;
    }

public:
    StringStreamBlockInputStream(const std::string col_name_) : col_name(col_name_) {}

    void append(const std::string & s) { data.emplace_back(s); }

    String getName() const override { return "StringStreamInput"; }

private:
    std::vector<std::string> data;
    const std::string col_name;
};

} // namespace DB
