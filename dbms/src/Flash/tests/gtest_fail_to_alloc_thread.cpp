#include <gtest/gtest.h>

#include <Flash/Mpp/ExchangeReceiver.cpp>

namespace DB
{
namespace tests
{
using Packet = mpp::MPPDataPacket;
using PacketPtr = std::shared_ptr<Packet>;
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
        String debugString() const
        {
            return "{Request}";
        }
        int send_task_id = 0;
    };

    struct Reader
    {
        Reader()
        {
        }

        void initialize() const
        {
        }

        bool read(mpp::MPPDataPacket * packet [[maybe_unused]]) const
        {
            return true;
        }

        Status finish() const
        {
            return {0, ""};
        }
    };

    MockReceiverContext()
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
        return std::make_shared<Reader>();
    }

    static Status getStatusOK()
    {
        return {0, ""};
    }
};
using MockExchangeReceiver = ExchangeReceiverBase<MockReceiverContext>;
class Fail_Alloc_Thread_Test : public ::testing::Test
{
public:
    void SetUp() override {}
};

TEST_F(Fail_Alloc_Thread_Test, ExchangeReceiver)
{
    tipb::ExchangeReceiver pb_exchange_receiver;
    mpp::TaskMeta task_meta;
    for (int i = 0; i < 5; ++i)
    {
        mpp::TaskMeta task;
        task.set_start_ts(0);
        task.set_task_id(i);
        task.set_partition_id(i);
        task.set_address("");

        String encoded_task;
        task.SerializeToString(&encoded_task);

        pb_exchange_receiver.add_encoded_task_meta(encoded_task);
    }
    std::vector<tipb::FieldType> fields(3);
    fields[0].set_tp(TiDB::TypeLongLong);
    fields[1].set_tp(TiDB::TypeString);
    fields[2].set_tp(TiDB::TypeLongLong);
    *pb_exchange_receiver.add_field_types() = fields[0];
    *pb_exchange_receiver.add_field_types() = fields[1];
    *pb_exchange_receiver.add_field_types() = fields[2];

    task_meta.set_task_id(100);

    FailPointHelper::enableFailPoint("exception_in_alloc_thread_in_exchange_receiver");
    try
    {
        auto exchange_receiver = std::make_shared<MockExchangeReceiver>(
            std::make_shared<MockReceiverContext>(),
            pb_exchange_receiver,
            task_meta,
            10,
            nullptr);
        exchange_receiver->init();
    }
    catch (Exception & e)
    {
    }
}

} // namespace tests
} // namespace DB
