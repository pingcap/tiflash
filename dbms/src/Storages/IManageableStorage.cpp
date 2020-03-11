#include <Storages/IManageableStorage.h>

namespace DB
{

void copyExecutorTreeWithLocalTableScan(tipb::DAGRequest & dag_req, const tipb::Executor * root)
{
    const tipb::Executor * current = root;
    auto * exec = dag_req.add_executors();
    while (current->tp() != tipb::ExecType::TypeTableScan)
    {
        if (current->tp() == tipb::ExecType::TypeSelection)
        {
            exec->set_tp(tipb::ExecType::TypeSelection);
            auto * sel = exec->mutable_selection();
            for (auto const & condition : current->selection().conditions())
            {
                auto * tmp = sel->add_conditions();
                tmp->CopyFrom(condition);
            }
            exec = sel->mutable_child();
            current = &current->selection().child();
        }
        else if (current->tp() == tipb::ExecType::TypeAggregation || current->tp() == tipb::ExecType::TypeStreamAgg)
        {
            exec->set_tp(current->tp());
            auto * agg = exec->mutable_aggregation();
            for (auto const & expr : current->aggregation().agg_func())
            {
                auto * tmp = agg->add_agg_func();
                tmp->CopyFrom(expr);
            }
            for (auto const & expr : current->aggregation().group_by())
            {
                auto * tmp = agg->add_group_by();
                tmp->CopyFrom(expr);
            }
            agg->set_streamed(current->aggregation().streamed());
            exec = agg->mutable_child();
            current = &current->aggregation().child();
        }
        else if (current->tp() == tipb::ExecType::TypeLimit)
        {
            exec->set_tp(current->tp());
            auto * limit = exec->mutable_limit();
            limit->set_limit(current->limit().limit());
            exec = limit->mutable_child();
            current = &current->limit().child();
        }
        else if (current->tp() == tipb::ExecType::TypeTopN)
        {
            exec->set_tp(current->tp());
            auto * topn = exec->mutable_topn();
            topn->set_limit(current->topn().limit());
            for (auto const & expr : current->topn().order_by())
            {
                auto * tmp = topn->add_order_by();
                tmp->CopyFrom(expr);
            }
            exec = topn->mutable_child();
            current = &current->topn().child();
        }
        else
        {
            throw Exception("Not supported yet");
        }
    }

    if (current->tp() != tipb::ExecType::TypeTableScan)
        throw Exception("Only support copy from table scan sourced query block");
    exec->set_tp(tipb::ExecType::TypeTableScan);
    auto * new_ts = new tipb::TableScan(current->tbl_scan());
    new_ts->set_next_read_engine(tipb::EngineType::Local);
    exec->set_allocated_tbl_scan(new_ts);

    dag_req.set_encode_type(tipb::EncodeType::TypeChunk);
}

BlockInputStreams IManageableStorage::remote_read(const std::vector<std::pair<DecodedTiKVKey, DecodedTiKVKey>> & key_ranges,
    UInt64 read_tso, const DAGQueryBlock & remote_query_block, Context & context)
{
    std::vector<pingcap::coprocessor::KeyRange> cop_key_ranges;
    for (const auto & key_range : key_ranges)
    {
        cop_key_ranges.push_back(
            pingcap::coprocessor::KeyRange{static_cast<String>(key_range.first), static_cast<String>(key_range.second)});
    }

    ::tipb::DAGRequest dag_req;

    copyExecutorTreeWithLocalTableScan(dag_req, remote_query_block.root);

    DAGSchema schema;
    ColumnsWithTypeAndName columns;
    for (int i = 0; i < (int)remote_query_block.output_field_types.size(); i++)
    {
        dag_req.add_output_offsets(i);
        ColumnInfo info = fieldTypeToColumnInfo(remote_query_block.output_field_types[i]);
        String col_name = "col_" + std::to_string(i);
        schema.push_back(std::make_pair(col_name, info));
        auto tp = getDataTypeByColumnInfo(info);
        ColumnWithTypeAndName col(tp, col_name);
        columns.emplace_back(col);
    }
    Block sample_block = Block(columns);

    pingcap::coprocessor::Request req;

    dag_req.SerializeToString(&req.data);
    req.tp = pingcap::coprocessor::ReqType::DAG;
    req.start_ts = read_tso;
    req.ranges = cop_key_ranges;

    pingcap::kv::Cluster * cluster = context.getTMTContext().getKVCluster();
    BlockInputStreamPtr input = std::make_shared<LazyBlockInputStream>(
        sample_block, [cluster, req, schema]() { return std::make_shared<CoprocessorBlockInputStream>(cluster, req, schema); });
    return {input};
};
} // namespace DB
