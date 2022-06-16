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
#include <TestUtils/mockExecutor.h>

namespace DB
{
namespace tests
{
class WindowFunctionBench : public ExchangeBench
{
public:
    void SetUp(const benchmark::State & state) override
    {
        // build tipb::Window and tipb::Sort.
        // select row_number() over w1 from t1 window w1 as (partition by c1, c2, c3 order by c1, c2, c3);
        ExchangeBench::SetUp(state);
        MockColumnInfos columns{
            {"c1", TiDB::TP::TypeLongLong},
            {"c2", TiDB::TP::TypeString},
            {"c3", TiDB::TP::TypeLongLong},
        };
        size_t executor_index = 0;
        DAGRequestBuilder builder(executor_index);
        builder
            .mockTable("test", "t1", columns)
            .sort({{"c1", false}, {"c2", false}, {"c3", false}}, true)
            .window(RowNumber(),
                    {{"c1", false}, {"c2", false}, {"c3", false}},
                    {{"c1", false}, {"c2", false}, {"c3", false}},
                    buildDefaultRowsFrame());
        tipb::DAGRequest req;
        MPPInfo mpp_info(0, -1, -1, {}, std::unordered_map<String, std::vector<Int64>>{});
        builder.getRoot()->toTiPBExecutor(req.mutable_root_executor(), /*collator_id=*/0, mpp_info, TiFlashTestEnv::getContext());
        assert(req.root_executor().tp() == tipb::TypeWindow);
        window = req.root_executor().window();
        assert(window.child().tp() == tipb::TypeSort);
        sort = window.child().sort();
    }

    void prepareWindowStream(Context & context, int concurrency, int source_num, int total_rows, const std::vector<Block> & blocks, BlockInputStreamPtr & sender_stream, BlockInputStreamPtr & receiver_stream, std::shared_ptr<SenderHelper> & sender_helper, std::shared_ptr<ReceiverHelper> & receiver_helper) const
    {
        DAGPipeline pipeline;
        receiver_helper = std::make_shared<ReceiverHelper>(concurrency, source_num);
        pipeline.streams = receiver_helper->buildExchangeReceiverStream();

        sender_helper = std::make_shared<SenderHelper>(source_num, concurrency, receiver_helper->queues, receiver_helper->fields);
        sender_stream = sender_helper->buildUnionStream(total_rows, blocks);

        context.setDAGContext(sender_helper->dag_context.get());
        std::vector<NameAndTypePair> source_columns{
            NameAndTypePair("c1", makeNullable(std::make_shared<DataTypeInt64>())),
            NameAndTypePair("c2", makeNullable(std::make_shared<DataTypeString>())),
            NameAndTypePair("c3", makeNullable(std::make_shared<DataTypeInt64>()))};
        auto mock_interpreter = mockInterpreter(context, source_columns, concurrency);
        mock_interpreter->input_streams_vec.push_back(pipeline.streams);
        mockExecuteWindowOrder(mock_interpreter, pipeline, sort);
        mockExecuteWindow(mock_interpreter, pipeline, window);
        pipeline.transform([&](auto & stream) {
            stream = std::make_shared<SquashingBlockInputStream>(stream, 8192, 0, "mock_executor_id_squashing");
        });
        receiver_stream = std::make_shared<UnionBlockInputStream<>>(pipeline.streams, nullptr, concurrency, /*req_id=*/"");
    }

    tipb::Window window;
    tipb::Sort sort;
};

BENCHMARK_DEFINE_F(WindowFunctionBench, basic_row_number)
(benchmark::State & state)
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

        prepareWindowStream(context, concurrency, source_num, total_rows, input_blocks, sender_stream, receiver_stream, sender_helper, receiver_helper);

        runAndWait(receiver_helper, receiver_stream, sender_helper, sender_stream);
    }
}
CATCH
BENCHMARK_REGISTER_F(WindowFunctionBench, basic_row_number)
    ->Args({8, 1, 1024 * 1000});

} // namespace tests
} // namespace DB
