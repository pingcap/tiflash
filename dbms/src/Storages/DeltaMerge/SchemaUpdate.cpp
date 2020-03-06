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

inline void setColumnDefineDefaultValue(const AlterCommand & command, ColumnDefine & define)
{
    std::function<Field(Field, DataTypePtr)> castDefaultValue; // for lazy bind
    castDefaultValue = [&](Field value, DataTypePtr type) -> Field {
        switch (type->getTypeId())
        {
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        {
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
                throw Exception("Unknown float number literal");
            }
        }
        case TypeIndex::FixedString:
        {
            String res = get<String>(value);
            return toField(res);
        }
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        {
            Int64 res = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
            return toField(res);
        }
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        {
            UInt64 res = applyVisitor(FieldVisitorConvertToNumber<UInt64>(), value);
            return toField(res);
        }
        case TypeIndex::DateTime:
        {
            auto                 date = safeGet<String>(value);
            time_t               time = 0;
            ReadBufferFromMemory buf(date.data(), date.size());
            readDateTimeText(time, buf);
            return toField((Int64)time);
        }
        case TypeIndex::Decimal32:
        {
            auto      dec   = std::dynamic_pointer_cast<const DataTypeDecimal32>(type);
            Int64     v     = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
            ScaleType scale = dec->getScale();
            return DecimalField(Decimal32(v), scale);
        }
        case TypeIndex::Decimal64:
        {
            auto      dec   = std::dynamic_pointer_cast<const DataTypeDecimal64>(type);
            Int64     v     = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
            ScaleType scale = dec->getScale();
            return DecimalField(Decimal64(v), scale);
        }
        case TypeIndex::Decimal128:
        {
            auto      dec   = std::dynamic_pointer_cast<const DataTypeDecimal128>(type);
            Int64     v     = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
            ScaleType scale = dec->getScale();
            return DecimalField(Decimal128(v), scale);
        }
        case TypeIndex::Decimal256:
        {
            auto      dec   = std::dynamic_pointer_cast<const DataTypeDecimal256>(type);
            Int64     v     = applyVisitor(FieldVisitorConvertToNumber<Int64>(), value);
            ScaleType scale = dec->getScale();
            return DecimalField(Decimal256(v), scale);
        }
        case TypeIndex::Nullable:
        {
            if (value.isNull())
                return value;
            auto        nullable    = std::dynamic_pointer_cast<const DataTypeNullable>(type);
            DataTypePtr nested_type = nullable->getNestedType();
            return castDefaultValue(value, nested_type);
        }
        default:
            throw Exception("Unsupported data type: " + type->getName());
        }
    };

    if (command.default_expression)
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
                setColumnDefineDefaultValue(command, column_define);
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
        }
        else
        {
            define.id = max_column_id_used++;
        }
        setColumnDefineDefaultValue(command, define);
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
    else
    {
        LOG_WARNING(log, __PRETTY_FUNCTION__ << " receive unknown alter command, type: " << DB::toString(static_cast<Int32>(command.type)));
    }
}

} // namespace DM
} // namespace DB