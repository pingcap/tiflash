#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/CHBlockChunkCodec.h>
#include <common/logger_useful.h>
#include <pingcap/coprocessor/Client.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>
#include <Flash/Coprocessor/DefaultChunkCodec.h>

namespace DB
{

class CoprocessorBlockInputStream : public IProfilingBlockInputStream
{
public:
    CoprocessorBlockInputStream(pingcap::kv::Cluster * cluster_, const pingcap::coprocessor::Request & req_, const DAGSchema & schema_,
            pingcap::kv::StoreType store_type)
        : req(req_),
          resp_iter(pingcap::coprocessor::Client::send(cluster_, &req, store_type)),
          schema(schema_),
          log(&Logger::get("pingcap/coprocessor"))
    {
        pingcap::Exception error = resp_iter.prepare();
        if (!error.empty())
        {
            LOG_WARNING(log, "coprocessor client meets error: " << error.displayText());
            throw error;
        }
    }

    Block getHeader() const override
    {
        ColumnsWithTypeAndName columns;
        for (auto name_and_column : schema)
        {
            auto tp = getDataTypeByColumnInfo(name_and_column.second);
            ColumnWithTypeAndName col(tp, name_and_column.first);
            columns.emplace_back(col);
        }
        LOG_DEBUG(log, "header columns: " + std::to_string(columns.size()));
        return Block(columns);
    }

    String getName() const override { return "Coprocessor"; }

    Block readImpl() override
    {
        if (chunk_queue.empty())
        {
            bool has_next = fetchNewData();
            if (!has_next)
                return {};
        }
        auto chunk = chunk_queue.front();
        chunk_queue.pop();
        switch (resp->encode_type()) {
            case tipb::EncodeType::TypeCHBlock:
                return CHBlockChunkCodec().decode(chunk, schema);
            case tipb::EncodeType::TypeChunk:
                return ArrowChunkCodec().decode(chunk, schema);
            case tipb::EncodeType::TypeDefault:
                return DefaultChunkCodec().decode(chunk, schema);
            default:
                throw Exception("Unsupported encode type", ErrorCodes::LOGICAL_ERROR);
        }
    }

private:
    bool fetchNewData()
    {
        LOG_DEBUG(log, "fetch new data");

        auto [data, has_next] = resp_iter.next();

        if (!has_next)
        {
            return false;
        }


        resp = std::make_shared<tipb::SelectResponse>();
        resp->ParseFromString(data);
        int chunks_size = resp->chunks_size();

        if (chunks_size == 0)
            return fetchNewData();

        for (int i = 0; i < chunks_size; i++)
        {
            chunk_queue.push(resp->chunks(i));
        }
        return true;
    }

    pingcap::coprocessor::Request req;
    pingcap::coprocessor::ResponseIter resp_iter;
    DAGSchema schema;

    std::shared_ptr<tipb::SelectResponse> resp;

    std::queue<tipb::Chunk> chunk_queue;

    Logger * log;
};

} // namespace DB