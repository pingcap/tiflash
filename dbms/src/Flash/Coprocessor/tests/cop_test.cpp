#include <grpc++/grpc++.h>
#include <sstream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#include <kvproto/tikvpb.grpc.pb.h>
#include <tipb/executor.pb.h>
#include <tipb/expression.pb.h>
#include <tipb/select.pb.h>
#pragma GCC diagnostic pop

#include <Storages/Transaction/Codec.h>


using ChannelPtr = std::shared_ptr<grpc::Channel>;
using SubPtr = std::shared_ptr<tikvpb::Tikv::Stub>;
static const int DAGREQUEST = 103;
class FlashClient
{
private:
    SubPtr sp;

public:
    static std::string decodeDatumToString(size_t & cursor, const std::string & raw_data)
    {
        switch (raw_data[cursor++])
        {
            case TiDB::CodecFlagNil:
                return "NULL";
            case TiDB::CodecFlagInt:
                return std::to_string(DB::DecodeInt<Int64>(cursor, raw_data));
            case TiDB::CodecFlagUInt:
                return std::to_string(DB::DecodeInt<UInt64>(cursor, raw_data));
            case TiDB::CodecFlagBytes:
                return DB::DecodeBytes(cursor, raw_data);
            case TiDB::CodecFlagCompactBytes:
                return DB::DecodeCompactBytes(cursor, raw_data);
            case TiDB::CodecFlagFloat:
                return std::to_string(DB::DecodeFloat64(cursor, raw_data));
            case TiDB::CodecFlagVarUInt:
                return std::to_string(DB::DecodeVarUInt(cursor, raw_data));
            case TiDB::CodecFlagVarInt:
                return std::to_string(DB::DecodeVarInt(cursor, raw_data));
            case TiDB::CodecFlagDuration:
                throw DB::Exception("Not implented yet. DecodeDatum: CodecFlagDuration");
            case TiDB::CodecFlagDecimal:
                return DB::DecodeDecimal(cursor, raw_data).toString();
            default:
                throw DB::Exception("Unknown Type:" + std::to_string(raw_data[cursor - 1]));
        }
    }

    FlashClient(ChannelPtr cp) : sp(tikvpb::Tikv::NewStub(cp)) {}
    grpc::Status coprocessor(coprocessor::Request * rqst, size_t output_column_num)
    {
        grpc::ClientContext clientContext;
        clientContext.AddMetadata("user_name", "");
        clientContext.AddMetadata("dag_planner", "optree");
        clientContext.AddMetadata("dag_expr_field_type_strict_check", "0");
        coprocessor::Response response;
        grpc::Status status = sp->Coprocessor(&clientContext, *rqst, &response);
        if (status.ok())
        {
            // if status is ok, try to decode the result
            tipb::SelectResponse selectResponse;
            if (selectResponse.ParseFromString(response.data()))
            {
                if (selectResponse.has_error())
                {
                    std::cout << "Coprocessor request failed, error code " << selectResponse.error().code() << " error msg "
                              << selectResponse.error().msg();
                    return status;
                }
                for (const tipb::Chunk & chunk : selectResponse.chunks())
                {
                    size_t cursor = 0;
                    const std::string & data = chunk.rows_data();
                    while (cursor < data.size())
                    {
                        for (size_t i = 0; i < output_column_num; i++)
                        {
                            std::cout << decodeDatumToString(cursor, data) << " ";
                        }
                        std::cout << std::endl;
                    }
                }
                std::cout << "Execute summary: " << std::endl;
                for (int i = 0; i < selectResponse.execution_summaries_size(); i++)
                {
                    auto & summary = selectResponse.execution_summaries(i);
                    std::cout << "Executor " << i;
                    std::cout << " time = " << summary.time_processed_ns() << " ns ";
                    std::cout << " rows = " << summary.num_produced_rows();
                    std::cout << " iter nums = " << summary.num_iterations();
                    std::cout << std::endl;
                }
            }
        }
        else
        {
            std::cout << "Coprocessor request failed, error code " << status.error_code() << " error msg " << status.error_message();
        }
        return status;
    }
};

using ClientPtr = std::shared_ptr<FlashClient>;

void appendTS(tipb::DAGRequest & dag_request, size_t & result_field_num)
{
    // table scan: s,i
    tipb::Executor * executor = dag_request.add_executors();
    executor->set_tp(tipb::ExecType::TypeTableScan);
    tipb::TableScan * ts = executor->mutable_tbl_scan();
    ts->set_table_id(44);
    tipb::ColumnInfo * ci = ts->add_columns();
    ci->set_column_id(1);
    ci->set_tp(0xfe);
    ci->set_flag(0);
    ci = ts->add_columns();
    ci->set_column_id(2);
    ci->set_tp(8);
    ci->set_flag(0);
    dag_request.add_output_offsets(1);
    dag_request.add_output_offsets(0);
    dag_request.add_output_offsets(1);
    result_field_num = 3;
}

void appendSelection(tipb::DAGRequest & dag_request)
{
    // selection: less(i, 123)
    auto * executor = dag_request.add_executors();
    executor->set_tp(tipb::ExecType::TypeSelection);
    tipb::Selection * selection = executor->mutable_selection();
    tipb::Expr * expr = selection->add_conditions();
    expr->set_tp(tipb::ExprType::ScalarFunc);
    expr->set_sig(tipb::ScalarFuncSig::LTInt);
    tipb::Expr * col = expr->add_children();
    tipb::Expr * value = expr->add_children();
    col->set_tp(tipb::ExprType::ColumnRef);
    std::stringstream ss;
    DB::EncodeNumber<Int64>(1, ss);
    col->set_val(ss.str());
    auto * type = col->mutable_field_type();
    type->set_tp(8);
    type->set_flag(0);
    value->set_tp(tipb::ExprType::Int64);
    ss.str("");
    DB::EncodeNumber<Int64>(10, ss);
    value->set_val(std::string(ss.str()));
    type = value->mutable_field_type();
    type->set_tp(8);
    type->set_flag(1);
    type = expr->mutable_field_type();
    type->set_tp(1);
    type->set_flag(1 << 5);

    // selection i in (5,10,11)
    selection->clear_conditions();
    expr = selection->add_conditions();
    expr->set_tp(tipb::ExprType::ScalarFunc);
    expr->set_sig(tipb::ScalarFuncSig::InInt);
    col = expr->add_children();
    col->set_tp(tipb::ExprType::ColumnRef);
    ss.str("");
    DB::EncodeNumber<Int64>(1, ss);
    col->set_val(ss.str());
    type = col->mutable_field_type();
    type->set_tp(8);
    type->set_flag(0);
    value = expr->add_children();
    value->set_tp(tipb::ExprType::Int64);
    ss.str("");
    DB::EncodeNumber<Int64>(10, ss);
    value->set_val(std::string(ss.str()));
    type = value->mutable_field_type();
    type->set_tp(8);
    type->set_flag(1);
    type = expr->mutable_field_type();
    type->set_tp(1);
    type->set_flag(1 << 5);
    value = expr->add_children();
    value->set_tp(tipb::ExprType::Int64);
    ss.str("");
    DB::EncodeNumber<Int64>(5, ss);
    value->set_val(std::string(ss.str()));
    type = value->mutable_field_type();
    type->set_tp(8);
    type->set_flag(1);
    type = expr->mutable_field_type();
    type->set_tp(1);
    type->set_flag(1 << 5);
    value = expr->add_children();
    value->set_tp(tipb::ExprType::Int64);
    ss.str("");
    DB::EncodeNumber<Int64>(11, ss);
    value->set_val(std::string(ss.str()));
    type = value->mutable_field_type();
    type->set_tp(8);
    type->set_flag(1);
    type = expr->mutable_field_type();
    type->set_tp(1);
    type->set_flag(1 << 5);

    // selection i is null
    /*
    selection->clear_conditions();
    expr = selection->add_conditions();
    expr->set_tp(tipb::ExprType::ScalarFunc);
    expr->set_sig(tipb::ScalarFuncSig::IntIsNull);
    col = expr->add_children();
    col->set_tp(tipb::ExprType::ColumnRef);
    ss.str("");
    DB::EncodeNumber<Int64>(1, ss);
    col->set_val(ss.str());
     */
}

void appendAgg(tipb::DAGRequest & dag_request, size_t & result_field_num)
{
    // agg: count(s) group by i;
    auto * executor = dag_request.add_executors();
    executor->set_tp(tipb::ExecType::TypeAggregation);
    auto agg = executor->mutable_aggregation();
    auto agg_func = agg->add_agg_func();
    agg_func->set_tp(tipb::ExprType::Count);
    auto child = agg_func->add_children();
    child->set_tp(tipb::ExprType::ColumnRef);
    std::stringstream ss;
    DB::EncodeNumber<Int64>(0, ss);
    child->set_val(ss.str());
    auto f_type = agg_func->mutable_field_type();
    f_type->set_tp(3);
    f_type->set_flag(33);
    auto group_col = agg->add_group_by();
    group_col->set_tp(tipb::ExprType::ColumnRef);
    ss.str("");
    DB::EncodeNumber<Int64>(1, ss);
    group_col->set_val(ss.str());
    f_type = group_col->mutable_field_type();
    f_type->set_tp(8);
    f_type->set_flag(1);
    result_field_num = 2;
}

void appendTopN(tipb::DAGRequest & dag_request)
{
    auto * executor = dag_request.add_executors();
    executor->set_tp(tipb::ExecType::TypeTopN);
    tipb::TopN * topN = executor->mutable_topn();
    topN->set_limit(3);
    tipb::ByItem * byItem = topN->add_order_by();
    byItem->set_desc(false);
    tipb::Expr * expr1 = byItem->mutable_expr();
    expr1->set_tp(tipb::ExprType::ColumnRef);
    std::stringstream ss;
    DB::EncodeNumber<Int64>(1, ss);
    expr1->set_val(ss.str());
    auto * type = expr1->mutable_field_type();
    type->set_tp(8);
    type->set_tp(0);
}

void appendLimit(tipb::DAGRequest & dag_request)
{
    auto * executor = dag_request.add_executors();
    executor->set_tp(tipb::ExecType::TypeLimit);
    tipb::Limit * limit = executor->mutable_limit();
    limit->set_limit(5);
}

grpc::Status rpcTest()
{
    ChannelPtr cp = grpc::CreateChannel("localhost:9093", grpc::InsecureChannelCredentials());
    ClientPtr clientPtr = std::make_shared<FlashClient>(cp);
    size_t result_field_num = 0;
    bool has_selection = true;
    bool has_agg = false;
    bool has_topN = true;
    bool has_limit = false;
    // construct a dag request
    tipb::DAGRequest dagRequest;
    dagRequest.set_start_ts(18446744073709551615uL);

    appendTS(dagRequest, result_field_num);
    if (has_selection)
        appendSelection(dagRequest);
    if (has_agg)
        appendAgg(dagRequest, result_field_num);
    if (has_topN)
        appendTopN(dagRequest);
    if (has_limit)
        appendLimit(dagRequest);

    // construct a coprocessor request
    coprocessor::Request request;
    //todo add context info
    kvrpcpb::Context * ctx = request.mutable_context();
    ctx->set_region_id(2);
    auto region_epoch = ctx->mutable_region_epoch();
    region_epoch->set_version(21);
    region_epoch->set_conf_ver(2);
    request.set_tp(DAGREQUEST);
    request.set_data(dagRequest.SerializeAsString());
    //request.add_ranges();
    return clientPtr->coprocessor(&request, result_field_num);
}

void codecTest()
{
    Int64 i = 123;
    std::stringstream ss;
    DB::EncodeNumber<Int64, TiDB::CodecFlag::CodecFlagInt>(i, ss);
    std::string val = ss.str();
    std::stringstream decode_ss;
    size_t cursor = 0;
    DB::Field f = DB::DecodeDatum(cursor, val);
    Int64 r = f.get<Int64>();
    r++;
}

int main()
{
    //    std::cout << "Before rpcTest"<< std::endl;
    grpc::Status ret = rpcTest();
    //      codecTest();
    //    std::cout << "End rpcTest " << std::endl;
    //    std::cout << "The ret is " << ret.error_code() << " " << ret.error_details()
    //    << " " << ret.error_message() << std::endl;
    return 0;
}
