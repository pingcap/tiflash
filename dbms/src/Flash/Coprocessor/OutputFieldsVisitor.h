#pragma once

#include <Flash/Coprocessor/TipbExecutorVisitor.h>

namespace DB
{
class OutputFieldsGenerator
{
public:
    static std::vector<tipb::FieldType> generate(const tipb::Executor & root)
    {
        std::vector<tipb::FieldType> output_field_types;
        TipbExecutorVisitor visitor;
        visitor.topDownVisit(root, [&](const tipb::Executor & e) { return visit(e, output_field_types); });
        return output_field_types;
    }

    static void generate(const ::google::protobuf::RepeatedPtrField<tipb::Executor> & executors)
    {
        std::vector<tipb::FieldType> output_field_types;
        TipbExecutorVisitor visitor;
        for (int i = (int)executors.size() - 1; i >= 0; i--)
            visitor.topDownVisit(executors[i], [&](const tipb::Executor & e) { return visit(e, output_field_types); });
        return output_field_types;
    }
private:
    static bool visit(const tipb::Executor & e, std::vector<tipb::FieldType> & field_types)
    {
        switch (e.tp())
        {
        case tipb::ExecType::TableScan:
            for (auto & ci : e.tbl_scan().columns())
            {
                tipb::FieldType field_type;
                field_type.set_tp(ci.tp());
                field_type.set_flag(ci.flag());
                field_type.set_flen(ci.columnlen());
                field_type.set_decimal(ci.decimal());
                for (const auto & elem : ci.elems())
                {
                    field_type.add_elems(elem);
                }
                field_types.push_back(field_type);
            }
            return true;
        case tipb::ExecType::TypeAggregation: // fallthrough
        case tipb::ExecType::TypeStreamAgg:
            for (auto & expr : e.aggregation().agg_func())
            {
                if (!exprHasValidFieldType(expr))
                {
                    throw TiFlashException("Agg expression without valid field type", Errors::Coprocessor::BadRequest);
                }
                field_types.push_back(expr.field_type());
            }
            for (auto & expr : e.aggregation().group_by())
            {
                if (!exprHasValidFieldType(expr))
                {
                    throw TiFlashException("Group by expression without valid field type", Errors::Coprocessor::BadRequest);
                }
                field_types.push_back(expr.field_type());
            }
            return true;
        case tipb::ExecType::TypeJoin:

        case tipb::ExecType::TypeExchangeSender:
            if (!e.exchange_sender().all_field_types().empty())
            {
                output_field_types.clear();
                for (auto & field_type : e.exchange_sender().all_field_types())
                    output_field_types.push_back(field_type);
                return true;
            }
            break;
        case tipb::ExecType::TypeExchangeReceiver:
            for (auto & field_type : e.exchange_receiver().field_types())
                output_field_types.push_back(field_type);
            return true;
        case tipb::ExecType::TypeProjection:
            for (auto & expr : e.projection().exprs())
                output_field_types.push_back(expr.field_type());
            return true;
        }
        return false;
    }

    static void collectOutputFieldsFromTableScan(const tipb::Executor & e, std::vector<tipb::FieldType> & field_types)
    {
    }
};
} // namespace DB
