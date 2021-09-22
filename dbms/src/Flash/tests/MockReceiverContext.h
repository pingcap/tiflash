#pragma once

#include <Flash/tests/Utils.h>

namespace DB::tests
{
struct MockReceiverContext
{
    struct Status
    {
        int status_code = 0;
        String error_msg;

        bool ok() const
        {
            return status_code == 0;
        }

        const String & error_message() const
        {
            return error_msg;
        }
    };

    struct Request
    {
        String DebugString() const
        {
            return "{Request}";
        }

        int send_task_id = 0;
    };

    struct Reader
    {
        explicit Reader(const PacketQueuePtr & queue_)
            : queue(queue_)
        {}

        void initialize() const
        {
        }

        bool read(mpp::MPPDataPacket * packet [[maybe_unused]]) const
        {
            auto res = queue->pop();
            if (!res.has_value() || !res.value())
                return false;
            *packet = *res.value();
            received_data_size.fetch_add(packet->ByteSizeLong());
            return true;
        }

        Status finish() const
        {
            return {0, ""};
        }

        PacketQueuePtr queue;
    };

    explicit MockReceiverContext(const std::vector<PacketQueuePtr> & queues_)
        : queues(queues_)
        , received_data_size(0)
    {
    }

    Request makeRequest(
        int index [[maybe_unused]],
        const tipb::ExchangeReceiver & pb_exchange_receiver [[maybe_unused]],
        const ::mpp::TaskMeta & task_meta [[maybe_unused]]) const
    {
        return {index};
    }

    std::shared_ptr<Reader> makeReader(const Request & request [[maybe_unused]])
    {
        return std::make_shared<Reader>(queues[request.send_task_id]);
    }

    static Status getStatusOK()
    {
        return {0, ""};
    }

    std::vector<PacketQueuePtr> queues;
    std::atomic<Int64> received_data_size;
};
} // DB::tests

