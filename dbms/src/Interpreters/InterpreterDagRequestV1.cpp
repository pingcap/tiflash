#include <DataStreams/BlockIO.h>
#include <Interpreters/InterpreterDagRequestV1.h>
#include <Core/QueryProcessingStage.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/CoprocessorBuilderUtils.h>
#include <Storages/Transaction/Types.h>
#include <Storages/Transaction/TMTContext.h>
#include <Storages/Transaction/SchemaSyncer.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/Transaction/Codec.h>

namespace DB {

    bool InterpreterDagRequestV1::buildTSString(const tipb::TableScan & ts, std::stringstream & ss) {
        TableID id;
        if(ts.has_table_id()) {
            id = ts.table_id();
        } else {
            // do not have table id
            return false;
        }
        auto & tmt_ctx = context.ch_context.getTMTContext();
        auto storage = tmt_ctx.getStorages().get(id);
        if(storage == nullptr) {
            tmt_ctx.getSchemaSyncer()->syncSchema(id, context.ch_context, false);
            storage = tmt_ctx.getStorages().get(id);
        }
        if(storage == nullptr) {
            return false;
        }
        const auto * merge_tree = dynamic_cast<const StorageMergeTree *>(storage.get());
        if (!merge_tree) {
            return false;
        }

        for(const tipb::ColumnInfo &ci : ts.columns()) {
            ColumnID cid = ci.column_id();
            String name = merge_tree->getTableInfo().columns[cid-1].name;
            column_name_from_ts.emplace(std::make_pair(cid, name));
        }
        if(column_name_from_ts.empty()) {
            // no column selected, must be something wrong
            return false;
        }
        ss << "FROM " << merge_tree->getTableInfo().db_name << "." << merge_tree->getTableInfo().name << " ";
        return true;
    }

    String InterpreterDagRequestV1::exprToString(const tipb::Expr & expr, bool &succ) {
        std::stringstream ss;
        succ = true;
        size_t cursor = 1;
        Int64 columnId = 0;
        String func_name;
        Field f;
        switch (expr.tp()) {
            case tipb::ExprType::Null:
                return "NULL";
            case tipb::ExprType::Int64:
                return std::to_string(DecodeInt<Int64>(cursor, expr.val()));
            case tipb::ExprType::Uint64:
                return std::to_string(DecodeInt<UInt64>(cursor, expr.val()));
            case tipb::ExprType::Float32:
            case tipb::ExprType::Float64:
                return std::to_string(DecodeFloat64(cursor, expr.val()));
            case tipb::ExprType::String:
                //
                return expr.val();
            case tipb::ExprType::Bytes:
                return DecodeBytes(cursor, expr.val());
            case tipb::ExprType::ColumnRef:
                columnId = DecodeInt<Int64>(cursor, expr.val());
                if(getCurrentColumnNames().count(columnId) == 0) {
                    succ = false;
                    return "";
                }
                return getCurrentColumnNames().find(columnId)->second;
            case tipb::ExprType::Count:
            case tipb::ExprType::Sum:
            case tipb::ExprType::Avg:
            case tipb::ExprType::Min:
            case tipb::ExprType::Max:
            case tipb::ExprType::First:
                if(!aggFunMap.count(expr.tp())) {
                    succ = false;
                    return "";
                }
                func_name = aggFunMap.find(expr.tp())->second;
                break;
            case tipb::ExprType::ScalarFunc:
                if(!scalarFunMap.count(expr.sig())) {
                    succ = false;
                    return "";
                }
                func_name = scalarFunMap.find(expr.sig())->second;
                break;
            default:
                succ = false;
                return "";
        }
        // build function expr
        if(func_name == "in") {
            // for in, we could not represent the function expr using func_name(param1, param2, ...)
            succ = false;
            return "";
        } else {
            ss << func_name << "(";
            bool first = true;
            bool sub_succ = true;
            for(const tipb::Expr &child : expr.children()) {
                String s = exprToString(child, sub_succ);
                if(!sub_succ) {
                    succ = false;
                    return "";
                }
                if(first) {
                    first = false;
                } else {
                    ss << ", ";
                }
                ss << s;
            }
            ss << ") ";
            return ss.str();
        }
    }

    bool InterpreterDagRequestV1::buildSelString(const tipb::Selection & sel, std::stringstream & ss) {
        bool first = true;
        for(const tipb::Expr & expr : sel.conditions()) {
            bool  succ = true;
            auto s = exprToString(expr, succ);
            if(!succ) {
                return false;
            }
            if(first) {
                ss << "WHERE ";
                first = false;
            } else {
                ss << "AND ";
            }
            ss << s << " ";
        }
        return true;
    }

    bool InterpreterDagRequestV1::buildLimitString(const tipb::Limit & limit, std::stringstream & ss) {
        ss << "LIMIT " << limit.limit() << " ";
        return true;
    }

    //todo return the error message
    bool InterpreterDagRequestV1::buildString(const tipb::Executor & executor, std::stringstream & ss) {
        switch (executor.tp()) {
            case tipb::ExecType::TypeTableScan:
                return buildTSString(executor.tbl_scan(), ss);
            case tipb::ExecType::TypeIndexScan:
                // index scan not supported
                return false;
            case tipb::ExecType::TypeSelection:
                return buildSelString(executor.selection(), ss);
            case tipb::ExecType::TypeAggregation:
                // stream agg is not supported, treated as normal agg
            case tipb::ExecType::TypeStreamAgg:
                //todo support agg
                return false;
            case tipb::ExecType::TypeTopN:
                // todo support top n
                return false;
            case tipb::ExecType::TypeLimit:
                return buildLimitString(executor.limit(), ss);
        }
    }

    bool isProject(const tipb::Executor &) {
        // currently, project is not pushed so always return false
        return false;
    }
    InterpreterDagRequestV1::InterpreterDagRequestV1(CoprocessorContext & context_, tipb::DAGRequest & dag_request_)
    : context(context_), dag_request(dag_request_) {
        afterAgg = false;
    }

    BlockIO InterpreterDagRequestV1::execute() {
        String query = buildSqlString();
        return executeQuery(query, context.ch_context, false, QueryProcessingStage::Complete);
    }

    String InterpreterDagRequestV1::buildSqlString() {
        std::stringstream query_buf;
        std::stringstream project;
        for(const tipb::Executor & executor : dag_request.executors()) {
            if(!buildString(executor, query_buf)) {
                return "";
            }
        }
        if(!isProject(dag_request.executors(dag_request.executors_size()-1))) {
            //append final project
            project << "SELECT ";
            bool first = true;
            for(UInt32 index : dag_request.output_offsets()) {
                if(first) {
                    first = false;
                } else {
                    project << ", ";
                }
                project << getCurrentColumnNames()[index+1];
            }
            project << " ";
        }
        return project.str() + query_buf.str();
    }

    InterpreterDagRequestV1::~InterpreterDagRequestV1() {

    }
}
