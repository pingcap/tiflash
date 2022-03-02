#pragma once

#include <Storages/Transaction/Types.h>

namespace DB
{
struct DAGProperties
    {
    String encode_type;
    Int64 tz_offset = 0;
    String tz_name;
    Int32 collator = 0;
    bool is_mpp_query = false;
    bool use_broadcast_join = false;
    Int32 mpp_partition_num = 1;
    Timestamp start_ts = DEFAULT_MAX_READ_TSO;
    Int32 mpp_timeout = 10;
    };
}