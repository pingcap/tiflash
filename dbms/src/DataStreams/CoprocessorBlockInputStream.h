#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Flash/Coprocessor/ArrowChunkCodec.h>

#include <common/logger_useful.h>
#include <pingcap/coprocessor/Client.h>

namespace DB {

class CoprocessorBlockInputStream : public IProfilingBlockInputStream {
public:
    CoprocessorBlockInputStream(pingcap::kv::Cluster * cluster_, const pingcap::coprocessor::Request & req_, const DAGSchema & schema_)
    : req(req_), resp_iter(pingcap::coprocessor::Client::send(cluster_, &req)), schema(schema_) {
        pingcap::Exception error = resp_iter.prepare();
        if (!error.empty())
            throw error;
    }

    Block getHeader() const override {
        return {};
    }

    String getName() const override { return "Coprocessor"; }

    Block readImpl() override {
        auto [data, has_next] = resp_iter.next();

        if (!has_next)
        {
            return {};
        }

        tipb::Chunk chunk;
        chunk.ParseFromString(data);

        Block block = codec.decode(chunk, schema);

        return block;
    }

private:
    pingcap::coprocessor::Request req;
    pingcap::coprocessor::ResponseIter resp_iter;
    DAGSchema schema;

    ArrowChunkCodec codec;
};

}