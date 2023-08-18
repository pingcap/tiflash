// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Common/DNSPTRResolverProvider.h>
#include <poll.h>

#include <mutex>
#include <span>

using ares_channel = struct ares_channeldata *;

namespace DB
{

/*
 * Implements reverse DNS resolution using c-ares lib. System reverse DNS resolution via
 * gethostbyaddr or getnameinfo does not work reliably because in some systems
 * it returns all PTR records for a given IP and in others it returns only one.
 *
 * Note that this class is defined under "Flash/" because it need to link/find header with c-ares
 * inside grpc
 **/
class CaresPTRResolver : public DNSPTRResolver
{
    friend class DNSPTRResolverProvider;

    /*
     * Allow only DNSPTRProvider to instantiate this class
     **/
    struct provider_token
    {
    };

    static constexpr auto C_ARES_POLL_EVENTS = POLLRDNORM | POLLIN;

public:
    explicit CaresPTRResolver(provider_token);
    ~CaresPTRResolver() override;

    std::unordered_set<std::string> resolve(const std::string & ip) override;

    std::unordered_set<std::string> resolve_v6(const std::string & ip) override;

private:
    bool wait_and_process();

    void cancel_requests();

    void resolve(const std::string & ip, std::unordered_set<std::string> & response);

    void resolve_v6(const std::string & ip, std::unordered_set<std::string> & response);

    std::span<pollfd> get_readable_sockets(int * sockets, pollfd * pollfd);

    int64_t calculate_timeout();

    void process_possible_timeout();

    void process_readable_sockets(std::span<pollfd> readable_sockets);

    ares_channel channel;

    static std::mutex mutex;
};

} // namespace DB
