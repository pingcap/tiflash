#pragma once

#include <string>
#include <memory>

#include <boost/noncopyable.hpp>

namespace DB
{

class TiDBService final : public std::enable_shared_from_this<TiDBService>, private boost::noncopyable
{
public:
    TiDBService(const std::string & service_ip_, const std::string & status_port_);
    const std::string & serviceIp() const;
    const std::string & statusPort() const;

private:
    const std::string service_ip;

    const std::string status_port;

};

} // namespace DB
