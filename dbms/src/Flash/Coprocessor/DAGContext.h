#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/Transaction/TiDB.h>

namespace DB
{

class Context;

/// A handy struct to get codec flag based on tp and flag.
struct FieldTpAndFlag
{
    TiDB::TP tp;
    UInt32 flag;

    TiDB::CodecFlag getCodecFlag() const
    {
        TiDB::ColumnInfo ci;
        ci.tp = tp;
        ci.flag = flag;
        return ci.getCodecFlag();
    }
};
using FieldTpAndFlags = std::vector<FieldTpAndFlag>;
class DAGContext
{
public:
    DAGContext() = default;
    std::vector<BlockInputStreams> & getProfileStreamsList() { return profile_streams_list; };
    FieldTpAndFlags & getResultFields() { return result_fields; };

private:
    std::vector<BlockInputStreams> profile_streams_list;
    FieldTpAndFlags result_fields;
};
} // namespace DB
