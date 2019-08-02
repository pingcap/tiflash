#include <sstream>
#include <grpc++/grpc++.h>

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
    FlashClient(ChannelPtr cp) : sp(tikvpb::Tikv::NewStub(cp)) {}
    grpc::Status coprocessor(coprocessor::Request * rqst)
    {
        grpc::ClientContext clientContext;
        clientContext.AddMetadata("user_name", "");
        clientContext.AddMetadata("builder_version", "v2");
        coprocessor::Response response;
        grpc::Status status = sp->Coprocessor(&clientContext, *rqst, &response);
        size_t column_num = 3;
        if (status.ok())
        {
            // if status is ok, try to decode the result
            tipb::SelectResponse selectResponse;
            if (selectResponse.ParseFromString(response.data()))
            {
                for (tipb::Chunk chunk : selectResponse.chunks())
                {
                    size_t cursor = 0;
                    std::vector<DB::Field> row_result;
                    const std::string & data = chunk.rows_data();
                    while (cursor < data.size())
                    {
                        row_result.push_back(DB::DecodeDatum(cursor, data));
                        if (row_result.size() == column_num)
                        {
                            //print the result
                            std::cout << row_result[0].get<DB::Int64>() << " " << row_result[1].get<DB::String>() << " "
                                      << row_result[2].get<DB::Int64>() << std::endl;
                            row_result.clear();
                        }
                    }
                }
            }
        }
        return status;
    }
};

using ClientPtr = std::shared_ptr<FlashClient>;
grpc::Status rpcTest()
{
    ChannelPtr cp = grpc::CreateChannel("localhost:9093", grpc::InsecureChannelCredentials());
    ClientPtr clientPtr = std::make_shared<FlashClient>(cp);
    // construct a dag request
    tipb::DAGRequest dagRequest;
    dagRequest.set_start_ts(18446744073709551615uL);
    tipb::Executor * executor = dagRequest.add_executors();
    executor->set_tp(tipb::ExecType::TypeTableScan);
    tipb::TableScan * ts = executor->mutable_tbl_scan();
    ts->set_table_id(41);
    tipb::ColumnInfo * ci = ts->add_columns();
    ci->set_column_id(1);
    ci = ts->add_columns();
    ci->set_column_id(2);
    dagRequest.add_output_offsets(1);
    dagRequest.add_output_offsets(0);
    dagRequest.add_output_offsets(1);
    executor = dagRequest.add_executors();
    executor->set_tp(tipb::ExecType::TypeSelection);
    tipb::Selection * selection = executor->mutable_selection();
    tipb::Expr * expr = selection->add_conditions();
    expr->set_tp(tipb::ExprType::ScalarFunc);
    expr->set_sig(tipb::ScalarFuncSig::LTInt);
    tipb::Expr * col = expr->add_children();
    tipb::Expr * value = expr->add_children();
    col->set_tp(tipb::ExprType::ColumnRef);
    std::stringstream ss;
    DB::EncodeNumber<Int64, TiDB::CodecFlagInt>(2, ss);
    col->set_val(ss.str());
    value->set_tp(tipb::ExprType::Int64);
    ss.str("");
    DB::EncodeNumber<Int64, TiDB::CodecFlagInt>(123, ss);
    value->set_val(std::string(ss.str()));

    // topn
    executor = dagRequest.add_executors();
    executor->set_tp(tipb::ExecType::TypeTopN);
    tipb::TopN * topN = executor->mutable_topn();
    topN->set_limit(3);
    tipb::ByItem * byItem = topN->add_order_by();
    byItem->set_desc(true);
    tipb::Expr * expr1 = byItem->mutable_expr();
    expr1->set_tp(tipb::ExprType::ColumnRef);
    ss.str("");
    DB::EncodeNumber<Int64, TiDB::CodecFlagInt>(2, ss);
    expr1->set_val(ss.str());
    // limit
    /*
    executor = dagRequest.add_executors();
    executor->set_tp(tipb::ExecType::TypeLimit);
    tipb::Limit *limit = executor->mutable_limit();
    limit->set_limit(1);
     */


    // construct a coprocessor request
    coprocessor::Request request;
    //todo add context info
    kvrpcpb::Context * ctx = request.mutable_context();
    ctx->set_region_id(2);
    auto region_epoch = ctx->mutable_region_epoch();
    region_epoch->set_version(20);
    region_epoch->set_conf_ver(2);
    request.set_tp(DAGREQUEST);
    request.set_data(dagRequest.SerializeAsString());
    //request.add_ranges();
    return clientPtr->coprocessor(&request);
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
