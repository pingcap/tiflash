#pragma once

#include <Flash/Mpp/MPPTask.h>
#include <common/logger_useful.h>
#include <kvproto/mpp.pb.h>

namespace DB
{
class MPPHandler
{
    const mpp::DispatchTaskRequest & task_request;

    Poco::Logger * log;

public:
    MPPHandler(const mpp::DispatchTaskRequest & task_request_)
        : task_request(task_request_)
        , log(&Poco::Logger::get("MPPHandler"))
    {}
    grpc::Status execute(Context & context, mpp::DispatchTaskResponse * response);
    void handleError(const MPPTaskPtr & task, String error);
};

} // namespace DB
