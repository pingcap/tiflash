#pragma once

#include <common/types.h>
#include <kvproto/disaggregated.pb.h>

namespace DB
{
class Context;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
} // namespace DB

namespace DB::S3
{

class S3LockClient
{
private:
    Context & context;
    LoggerPtr log;

public:
    explicit S3LockClient(Context & context_);

    std::pair<bool, std::optional<disaggregated::S3LockResult>>
    sendTryAddLockRequest(String address, int timeout, const String & ori_data_file, UInt32 lock_store_id, UInt32 lock_seq);

    std::pair<bool, std::optional<disaggregated::S3LockResult>>
    sendTryMarkDeleteRequest(String address, int timeout, const String & ori_data_file);
};

} // namespace DB::S3
