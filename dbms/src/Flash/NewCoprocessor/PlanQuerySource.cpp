#include <Flash/Coprocessor/InterpreterDAG.h>
#include <Flash/NewCoprocessor/InterpreterPlan.h>
#include <Flash/NewCoprocessor/PlanQuerySource.h>
#include <Flash/Plan/Plans.h>
#include <Flash/Plan/toPlan.h>
#include <Parsers/makeDummyQuery.h>
#include <fmt/format.h>

namespace DB
{
PlanQuerySource::PlanQuerySource(Context & context_)
    : context(context_)
{
    const tipb::DAGRequest & dag_request = *getDAGContext().dag_request;
    root_plan = toPlan(dag_request);

    for (Int32 i : dag_request.output_offsets())
        output_offsets.push_back(i);

    assert(root_plan->tp() == tipb::TypeExchangeSender);
    root_plan->toImpl<ExchangeSenderPlan>([&](const ExchangeSenderPlan & sender) {
        assert(!sender.impl.all_field_types().empty());
        for (const auto & field_type : sender.impl.all_field_types())
        {
            output_field_types.push_back(field_type);
        }
    });

    for (UInt32 i : dag_request.output_offsets())
    {
        if (unlikely(i >= output_field_types.size()))
            throw TiFlashException(
                fmt::format(
                    "{}: Invalid output offset(schema has {} columns, access index {}",
                    __PRETTY_FUNCTION__,
                    output_field_types.size(),
                    i),
                Errors::Coprocessor::BadRequest);
        getDAGContext().result_field_types.push_back(output_field_types[i]);
    }

    auto encode_type = analyzeDAGEncodeType(getDAGContext());
    getDAGContext().encode_type = encode_type;
    getDAGContext().keep_session_timezone_info = encode_type == tipb::EncodeType::TypeChunk || encode_type == tipb::EncodeType::TypeCHBlock;
}

std::tuple<std::string, ASTPtr> PlanQuerySource::parse(size_t)
{
    // this is a WAR to avoid NPE when the MergeTreeDataSelectExecutor trying
    // to extract key range of the query.
    // todo find a way to enable key range extraction for dag query
    return {getDAGContext().dag_request->DebugString(), makeDummyQuery()};
}

String PlanQuerySource::str(size_t)
{
    return getDAGContext().dag_request->DebugString();
}

std::unique_ptr<IInterpreter> PlanQuerySource::interpreter(Context &, QueryProcessingStage::Enum)
{
    return std::make_unique<InterpreterPlan>(context, *this);
}

} // namespace DB
