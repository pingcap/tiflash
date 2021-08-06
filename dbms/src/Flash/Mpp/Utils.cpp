#include <Flash/Mpp/Utils.h>
#include <memory>

namespace DB
{

mpp::MPPDataPacket getPacketWithError(String reason)
{
    mpp::MPPDataPacket data;
    auto err = std::make_unique<mpp::Error>();
    err->set_msg(std::move(reason));
    data.set_allocated_error(err.release());
    return data;
}

} // namespace DB

