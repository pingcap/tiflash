// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Flash/tests/bench_exchange.h>

namespace DB
{
namespace tests
{

std::string window_json = R"({"funcDesc":[{"tp":"RowNumber","sig":"Unspecified","fieldType":{"tp":8,"flag":128,"flen":21,"decimal":-1,"collate":-63,"charset":"binary"},"hasDistinct":false}],"partitionBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":100,"decimal":0,"collate":-46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},"hasDistinct":false},"desc":false}],"orderBy":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},"hasDistinct":false},"desc":false}],"frame":{"type":"Rows","start":{"type":"CurrentRow","unbounded":false,"offset":"0"},"end":{"type":"CurrentRow","unbounded":false,"offset":"0"}},"child":{"tp":"TypeSort","executorId":"Sort_17","sort":{"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":100,"decimal":0,"collate":-46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAkPqbr7iCBhABIg8xMjcuMC4wLjE6NDA3Njk="],"fieldTypes":[{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},{"tp":15,"flag":0,"flen":100,"decimal":0,"collate":-46,"charset":"utf8mb4"},{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"}]},"executorId":"ExchangeReceiver_16"}}}})";
std::string sort_json = R"({"byItems":[{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAE=","sig":"Unspecified","fieldType":{"tp":15,"flag":0,"flen":100,"decimal":0,"collate":-46,"charset":"utf8mb4"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAI=","sig":"Unspecified","fieldType":{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},"hasDistinct":false},"desc":false},{"expr":{"tp":"ColumnRef","val":"gAAAAAAAAAA=","sig":"Unspecified","fieldType":{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},"hasDistinct":false},"desc":false}],"isPartialSort":true,"child":{"tp":"TypeExchangeReceiver","exchangeReceiver":{"encodedTaskMeta":["CIGAkPqbr7iCBhABIg8xMjcuMC4wLjE6NDA3Njk="],"fieldTypes":[{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"},{"tp":15,"flag":0,"flen":100,"decimal":0,"collate":-46,"charset":"utf8mb4"},{"tp":8,"flag":0,"flen":20,"decimal":0,"collate":-63,"charset":"binary"}]},"executorId":"ExchangeReceiver_16"}})";
void prepareWindowStream(Context & context, int concurrency, int source_num,
        int total_rows, const std::vector<Block> & blocks,
        BlockInputStreamPtr & sender_stream, BlockInputStreamPtr & receiver_stream,
        std::shared_ptr<SenderHelper> & sender_helper, std::shared_ptr<ReceiverHelper> & receiver_helper)
{
    DAGPipeline pipeline;
    receiver_helper = std::make_shared<ReceiverHelper>(concurrency, source_num);
    pipeline.streams = receiver_helper->buildExchangeReceiverStream();

    sender_helper = std::make_shared<SenderHelper>(source_num, concurrency,
            receiver_helper->queues, receiver_helper->fields);
    sender_stream = sender_helper->buildUnionStream(total_rows, blocks);

    context.setDAGContext(sender_helper->dag_context.get());
    std::vector<NameAndTypePair> source_columns{
        NameAndTypePair("c1", makeNullable(std::make_shared<DataTypeInt64>())),
        NameAndTypePair("c2", makeNullable(std::make_shared<DataTypeString>())),
        NameAndTypePair("c3", makeNullable(std::make_shared<DataTypeInt64>()))
    };
    auto mock_interpreter = mockInterpreter(context, source_columns, concurrency);
    mock_interpreter->input_streams_vec.push_back(pipeline.streams);
    mockExecuteWindowOrder(mock_interpreter, pipeline, sort_json);
    mockExecuteWindow(mock_interpreter, pipeline, window_json);
    pipeline.transform([&](auto & stream) {
        stream = std::make_shared<SquashingBlockInputStream>(stream, 8192, 0, "mock_executor_id_squashing");
    });
    receiver_stream = std::make_shared<UnionBlockInputStream<>>(pipeline.streams, nullptr, concurrency, /*req_id=*/"");
}

class WindowFunctionBench : public ExchangeBench
{
};

BENCHMARK_DEFINE_F(WindowFunctionBench, basic_row_number)(benchmark::State & state)
try
{
    const int concurrency = state.range(0);
    const int source_num = state.range(1);
    const int total_rows = state.range(2);
    Context context = TiFlashTestEnv::getContext();

    for (auto _ : state)
    {
        std::shared_ptr<SenderHelper> sender_helper;
        std::shared_ptr<ReceiverHelper> receiver_helper;
        BlockInputStreamPtr sender_stream;
        BlockInputStreamPtr receiver_stream;

        prepareWindowStream(context, concurrency, source_num, total_rows, input_blocks,
                sender_stream, receiver_stream,
                sender_helper, receiver_helper);

        runAndWait(receiver_helper, receiver_stream, sender_helper, sender_stream);
    }
}
CATCH
BENCHMARK_REGISTER_F(WindowFunctionBench, basic_row_number)
    ->Args({8, 1, 1024*1000});

} // namespace tests
} // namespace DB
