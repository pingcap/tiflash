#include <Common/Logger.h>
#include <Flash/Disaggregated/S3LockClient.h>
#include <Interpreters/Context.h>
#include <Storages/Transaction/TMTContext.h>
#include <common/defines.h>
#include <common/types.h>
#include <grpcpp/client_context.h>
#include <pingcap/kv/Rpc.h>
#include <pingcap/kv/internal/conn.h>

namespace pingcap::kv
{
// The rpc trait
template <>
struct RpcTypeTraits<disaggregated::TryAddLockRequest>
{
    using RequestType = disaggregated::TryAddLockRequest;
    using ResultType = disaggregated::TryAddLockResponse;
    static const char * err_msg() { return "tryAddLock Failed"; } // NOLINT(readability-identifier-naming)
    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        ResultType * res)
    {
        return client->stub->tryAddLock(context, req, res);
    }
};

template <>
struct RpcTypeTraits<disaggregated::TryMarkDeleteRequest>
{
    using RequestType = disaggregated::TryMarkDeleteRequest;
    using ResultType = disaggregated::TryMarkDeleteResponse;
    static const char * err_msg() { return "tryMarkDelete Failed"; } // NOLINT(readability-identifier-naming)
    static ::grpc::Status doRPCCall(
        grpc::ClientContext * context,
        std::shared_ptr<KvConnClient> client,
        const RequestType & req,
        ResultType * res)
    {
        return client->stub->tryMarkDelete(context, req, res);
    }
};

} // namespace pingcap::kv


namespace DB::S3
{
S3LockClient::S3LockClient(Context & context_)
    : context(context_)
    , log(Logger::get())
{
    UNUSED(context);
}

std::pair<bool, std::optional<disaggregated::S3LockError>> S3LockClient::sendTryAddLockRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id, UInt32 lock_store_id, UInt32 upload_seq)
{
    auto req = std::make_shared<disaggregated::TryAddLockRequest>();
    req->set_ori_data_file(ori_data_file);
    req->set_ori_store_id(ori_store_id);
    req->set_lock_store_id(lock_store_id);
    req->set_upload_seq(upload_seq);

    auto res = std::make_shared<disaggregated::TryAddLockResponse>();
    auto call = pingcap::kv::RpcCall<disaggregated::TryAddLockRequest>(req);
    auto * cluster = context.getTMTContext().getKVCluster();
    LOG_DEBUG(log, "Send TryAddLock request, address={} req={}", address, req->DebugString());
    cluster->rpc_client->sendRequest(address, call, timeout);
    const auto & resp = call.getResp();
    LOG_DEBUG(log, "Received TryAddLock response, resp={}", resp->DebugString());
    if (resp->has_error())
    {
        LOG_ERROR(log, "TryMarkDelete get resp with error={}", resp->error().DebugString());
        return {false, resp->error()};
    }

    return {resp->is_success(), std::nullopt};
}

std::pair<bool, std::optional<disaggregated::S3LockError>> S3LockClient::sendTryMarkDeleteRequest(String address, int timeout, const String & ori_data_file, UInt32 ori_store_id)
{
    auto req = std::make_shared<disaggregated::TryMarkDeleteRequest>();
    req->set_ori_data_file(ori_data_file);
    req->set_ori_store_id(ori_store_id);

    auto res = std::make_shared<disaggregated::TryMarkDeleteResponse>();
    auto call = pingcap::kv::RpcCall<disaggregated::TryMarkDeleteRequest>(req);
    auto * cluster = context.getTMTContext().getKVCluster();
    LOG_DEBUG(log, "Send TryMarkDelete request, address={} req={}", address, req->DebugString());
    cluster->rpc_client->sendRequest(address, call, timeout);
    const auto & resp = call.getResp();
    LOG_DEBUG(log, "Received TryMarkDelete response, resp={}", resp->DebugString());
    if (resp->has_error())
    {
        LOG_ERROR(log, "TryMarkDelete get resp with error={}", resp->error().DebugString());
        return {false, resp->error()};
    }

    return {resp->is_success(), std::nullopt};
}
} // namespace DB::S3
