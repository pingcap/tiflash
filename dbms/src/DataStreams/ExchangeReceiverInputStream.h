#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <Flash/Mpp/ExchangeReceiver.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/logger_useful.h>

#include <chrono>
#include <mutex>
#include <thread>
#include <utility>

namespace DB
{

// ExchangeReceiver is in charge of receiving data from exchangeSender located in upstream tasks.
class ExchangeReceiverInputStream : public IProfilingBlockInputStream
{
    std::shared_ptr<ExchangeReceiver> receiver;

    Block sample_block;

public:
    ExchangeReceiverInputStream(std::shared_ptr<ExchangeReceiver> receiver_) : receiver(std::move(receiver_))
    {
        // generate sample block
        ColumnsWithTypeAndName columns;
        for (auto & dag_col : receiver->getOutputSchema())
        {
            auto tp = getDataTypeByColumnInfo(dag_col.second);
            ColumnWithTypeAndName col(tp, dag_col.first);
            columns.emplace_back(col);
        }
        sample_block = Block(columns);
    }

    Block getHeader() const override { return sample_block; }

    String getName() const override { return "ExchangeReceiver"; }

    Block readImpl() override { return receiver->nextBlock(); }
};
} // namespace DB
