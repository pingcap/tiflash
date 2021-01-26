#include <Common/FieldVisitors.h>
#include <Functions/FunctionsConversion.h>
#include <IO/ReadBufferFromMemory.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/DeltaMerge/SchemaUpdate.h>
#include <Storages/Transaction/TiDB.h>
#include <common/logger_useful.h>

namespace DB
{
namespace DM
{

String astToDebugString(const IAST * const ast)
{
    std::stringstream ss;
    ast->dumpTree(ss);
    return ss.str();
}

inline void setColumnDefineDefaultValue(const AlterCommand & command, ColumnDefine & define)
{
    std::function<Field(const Field &, const DataTypePtr &)> castDefaultValue; // for lazy bind
    castDefaultValue = [&](const Field & value, const DataTypePtr & type) -> Field {
        switch (type->getTypeId())
        {
        case TypeIndex::Float32:
        case TypeIndex::Float64: {
            if (value.getType() == Field::Types::Float64)
            {
                Float64 res = applyVisitor(FieldVisitorConvertToNumber<Float64>(), value);
                return toField(res);
            }
            else if (value.getType() == Field::Types::Decimal32)
            {
                DecimalField<Decimal32> dec = safeGet<DecimalField<Decimal32>>(value);
                Float64                 res = dec.getValue().toFloat<Float64>(dec.getScale());
                return toField(res);
            }
            else if (value.getType() == Field::Types::Decimal64)
            {
                DecimalField<Decimal64> dec = safeGet<DecimalField<Decimal64>>(value);
                Float64                 res = dec.getValue().toFloat<Float64>(dec.getScale());
                return toField(res);
            }
            else
            {
                throw Exception("Unknown float number literal: " + applyVisitor(FieldVisitorToString(), value)
                                + ", value type: " + value.getTypeName());
            }
        }
        case TypeIndex::String:
        case TypeIndex::FixedString: {
            String res = get<String>(value);
            return toField(res);
        }
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64: {
            Int64 res = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
            return toField(res);
        }
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64: {
            UInt64 res = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), value);
            return toField(res);
        }
        case TypeIndex::DateTime: {
            auto                 date = safeGet<String>(value);
            time_t               time = 0;
            ReadBufferFromMemory buf(date.data(), date.size());
            readDateTimeText(time, buf);
            return toField((Int64)time);
        }
        case TypeIndex::Decimal32: {
            auto v = safeGet<DecimalField<Decimal32>>(value);
            return v;
        }
        case TypeIndex::Decimal64: {
            auto v = safeGet<DecimalField<Decimal64>>(value);
            return v;
        }
        case TypeIndex::Decimal128: {
            auto v = safeGet<DecimalField<Decimal128>>(value);
            return v;
        }
        case TypeIndex::Decimal256: {
            auto v = safeGet<DecimalField<Decimal256>>(value);
            return v;
        }
        case TypeIndex::Enum16: {
            // According to `Storages/Transaction/TiDB.h` and MySQL 5.7
            // document(https://dev.mysql.com/doc/refman/5.7/en/enum.html),
            // enum support 65,535 distinct value at most, so only Enum16 is supported here.
            // Default value of Enum should be store as a Int64 Field (Storages/Transaction/Datum.cpp)
            Int64 res = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
            return toField(res);
        }
        case TypeIndex::MyDate:
        case TypeIndex::MyDateTime: {
            static_assert(std::is_same_v<DataTypeMyDate::FieldType, UInt64>);
            static_assert(std::is_same_v<DataTypeMyDateTime::FieldType, UInt64>);
            UInt64 res = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), value);
            return toField(res);
        }
        case TypeIndex::Nullable: {
            if (value.isNull())
                return value;
            auto        nullable    = std::dynamic_pointer_cast<const DataTypeNullable>(type);
            DataTypePtr nested_type = nullable->getNestedType();
            return castDefaultValue(value, nested_type); // Recursive call on nested type
        }
        default:
            throw Exception("Unsupported to setColumnDefineDefaultValue with data type: " + type->getName()
                            + " value: " + applyVisitor(FieldVisitorToString(), value) + ", type: " + value.getTypeName());
        }
    };

    if (command.default_expression)
    {
        try
        {
            // a cast function
            // change column_define.default_value

            if (auto default_literal = typeid_cast<const ASTLiteral *>(command.default_expression.get());
                default_literal && default_literal->value.getType() == Field::Types::String)
            {
                define.default_value = default_literal->value;
            }
            else if (auto default_cast_expr = typeid_cast<const ASTFunction *>(command.default_expression.get());
                     default_cast_expr && default_cast_expr->name == "CAST" /* ParserCastExpression::name */)
            {
                // eg. CAST('1.234' AS Float32); CAST(999 AS Int32)
                if (default_cast_expr->arguments->children.size() != 2)
                {
                    throw Exception("Unknown CAST expression in default expr", ErrorCodes::NOT_IMPLEMENTED);
                }

                auto default_literal_in_cast = typeid_cast<const ASTLiteral *>(default_cast_expr->arguments->children[0].get());
                if (default_literal_in_cast)
                {
                    Field default_value  = castDefaultValue(default_literal_in_cast->value, define.type);
                    define.default_value = default_value;
                }
                else
                {
                    throw Exception("Invalid CAST expression", ErrorCodes::BAD_ARGUMENTS);
                }
            }
            else
            {
                throw Exception("Default value must be a string or CAST('...' AS WhatType)", ErrorCodes::BAD_ARGUMENTS);
            }
        }
        catch (DB::Exception & e)
        {
            e.addMessage("(in setColumnDefineDefaultValue for default_expression:" + astToDebugString(command.default_expression.get())
                         + ")");
            throw;
        }
        catch (const Poco::Exception & e)
        {
            DB::Exception ex(e);
            ex.addMessage("(in setColumnDefineDefaultValue for default_expression:" + astToDebugString(command.default_expression.get())
                          + ")");
            throw ex;
        }
        catch (std::exception & e)
        {
            std::stringstream ss;
            ss << "std::exception: " << e.what()
               << " (in setColumnDefineDefaultValue for default_expression:" + astToDebugString(command.default_expression.get()) << ")";
            DB::Exception ex(ss.str(), ErrorCodes::LOGICAL_ERROR);
            throw ex;
        }
    }
}


inline void setColumnDefineDefaultValue(const TiDB::TableInfo & table_info, ColumnDefine & define)
{
    // Check ConvertColumnType_test.GetDefaultValue for unit test.
    const auto & col_info = table_info.getColumnInfo(define.id);
    define.default_value  = col_info.defaultValueToField();
}

void applyAlter(ColumnDefines &               table_columns,
                const AlterCommand &          command,
                const OptionTableInfoConstRef table_info,
                ColumnID &                    max_column_id_used)
{
    /// Caller should ensure the command is legal.
    /// eg. The column to modify/drop/rename must exist, the column to add must not exist, the new column name of rename must not exists.

    Logger * log = &Logger::get("SchemaUpdate");

    if (command.type == AlterCommand::MODIFY_COLUMN)
    {
        // find column define and then apply modify
        bool exist_column = false;
        for (auto && column_define : table_columns)
        {
            if (column_define.id == command.column_id)
            {
                exist_column       = true;
                column_define.type = command.data_type;
                if (table_info)
                {
                    setColumnDefineDefaultValue(*table_info, column_define);
                }
                else
                {
                    setColumnDefineDefaultValue(command, column_define);
                }
                break;
            }
        }
        if (unlikely(!exist_column))
        {
            // Fall back to find column by name, this path should only call by tests.
            LOG_WARNING(log,
                        "Try to apply alter to column: " << command.column_name << ", id:" << DB::toString(command.column_id)
                                                         << ", but not found by id, fall back locating col by name.");
            for (auto && column_define : table_columns)
            {
                if (column_define.name == command.column_name)
                {
                    exist_column       = true;
                    column_define.type = command.data_type;
                    setColumnDefineDefaultValue(command, column_define);
                    break;
                }
            }
            if (unlikely(!exist_column))
            {
                throw Exception(String("Alter column: ") + command.column_name + " is not exists.", ErrorCodes::LOGICAL_ERROR);
            }
        }
    }
    else if (command.type == AlterCommand::ADD_COLUMN)
    {
        // we don't care about `after_column` in `store_columns`

        /// If TableInfo from TiDB is not empty, we get column id from TiDB
        /// else we allocate a new id by `max_column_id_used`
        ColumnDefine define(0, command.column_name, command.data_type);
        if (table_info)
        {
            define.id = table_info->get().getColumnID(command.column_name);
            setColumnDefineDefaultValue(*table_info, define);
        }
        else
        {
            define.id = max_column_id_used++;
            setColumnDefineDefaultValue(command, define);
        }
        table_columns.emplace_back(std::move(define));
    }
    else if (command.type == AlterCommand::DROP_COLUMN)
    {
        table_columns.erase(
            std::remove_if(table_columns.begin(), table_columns.end(), [&](const ColumnDefine & c) { return c.id == command.column_id; }),
            table_columns.end());
    }
    else if (command.type == AlterCommand::RENAME_COLUMN)
    {
        for (auto && c : table_columns)
        {
            if (c.id == command.column_id)
            {
                c.name = command.new_column_name;
                break;
            }
        }
    }
    else if (command.type == AlterCommand::TOMBSTONE || command.type == AlterCommand::RECOVER)
    {
        // Nothing to do.
    }
    else
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << " receive unknown alter command, type: " << DB::toString(static_cast<Int32>(command.type)));
    }
}

} // namespace DM
} // namespace DB
