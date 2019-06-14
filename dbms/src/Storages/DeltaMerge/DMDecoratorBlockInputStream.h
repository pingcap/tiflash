#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionsConversion.h>
#include <Storages/DeltaMerge/DeltaMergeHelpers.h>
#include <unordered_set>


namespace DB
{

class DMDecoratorBlockInputStream : public IProfilingBlockInputStream
{
public:
    using ColumnNames = std::unordered_set<std::string>;

    DMDecoratorBlockInputStream(const BlockInputStreamPtr & input,
                                const ColumnDefines &       columns_to_read,
                                const String &              handle_name_,
                                const DataTypePtr &         handle_original_type_,
                                const Context &             context_)
        : handle_name(handle_name_), handle_original_type(handle_original_type_), context(context_)
    {
        children.emplace_back(input);
        for (auto & col : columns_to_read)
        {
            column_names.emplace(col.name);
            addColumn(header, col.id, col.name, col.type, col.type->createColumn());
        }
    }

    String getName() const override { return "DMDecorator"; }

    Block getHeader() const override { return header; }

protected:
    Block readImpl() override
    {
        Block res = children.back()->read();
        if (!res)
            return {};
        auto all_names = res.getNames();
        for (auto & n : all_names)
        {
            if (!column_names.count(n))
                res.erase(n);
        }

        if (handle_original_type && column_names.count(handle_name))
        {
            auto pos = res.getPositionByName(handle_name);
            convert(res, pos, handle_original_type, context);
            res.getByPosition(pos).type = handle_original_type;
        }

        return res;
    }

    static void convert(Block & block, size_t pos, const DataTypePtr & to_type, const Context & context)
    {
        auto * to_type_ptr = &(*to_type);

        if (checkDataType<DataTypeUInt8>(to_type_ptr))
            FunctionToUInt8::create(context)->execute(block, {pos}, pos);
        else if (checkDataType<DataTypeUInt16>(to_type_ptr))
            FunctionToUInt16::create(context)->execute(block, {pos}, pos);
        else if (checkDataType<DataTypeUInt32>(to_type_ptr))
            FunctionToUInt32::create(context)->execute(block, {pos}, pos);
        else if (checkDataType<DataTypeUInt64>(to_type_ptr))
            FunctionToUInt64::create(context)->execute(block, {pos}, pos);
        else if (checkDataType<DataTypeInt8>(to_type_ptr))
            FunctionToInt8::create(context)->execute(block, {pos}, pos);
        else if (checkDataType<DataTypeInt16>(to_type_ptr))
            FunctionToInt16::create(context)->execute(block, {pos}, pos);
        else if (checkDataType<DataTypeInt32>(to_type_ptr))
            FunctionToInt32::create(context)->execute(block, {pos}, pos);
        else if (checkDataType<DataTypeInt64>(to_type_ptr))
            FunctionToInt64::create(context)->execute(block, {pos}, pos);
        else
            throw Exception("Forgot to support type: " + to_type->getName());
    }

private:
    ColumnNames     column_names;
    Block           header;
    String          handle_name;
    DataTypePtr     handle_original_type;
    const Context & context;
};

} // namespace DB
