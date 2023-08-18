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

#include <common/types.h>
#include <etcd/rpc.grpc.pb.h>
#include <etcd/v3election.grpc.pb.h>
#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <pingcap/Config.h>

#include <memory>

namespace DB::Etcd
{

struct EtcdConnClient;
using EtcdConnClientPtr = std::shared_ptr<EtcdConnClient>;


struct EtcdConnClient
{
    std::shared_ptr<grpc::Channel> channel;
    std::unique_ptr<etcdserverpb::KV::Stub> kv_stub;
    std::unique_ptr<etcdserverpb::Lease::Stub> lease_stub;
    std::unique_ptr<etcdserverpb::Watch::Stub> watch_stub;
    std::unique_ptr<v3electionpb::Election::Stub> election_stub;

    EtcdConnClient(const String & addr, const pingcap::ClusterConfig & config)
    {
        if (config.hasTlsConfig())
        {
            channel = grpc::CreateChannel(addr, grpc::SslCredentials(config.getGrpcCredentials()));
        }
        else
        {
            channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        }
        kv_stub = etcdserverpb::KV::NewStub(channel);
        lease_stub = etcdserverpb::Lease::NewStub(channel);
        watch_stub = etcdserverpb::Watch::NewStub(channel);
        election_stub = v3electionpb::Election::NewStub(channel);
    }
};

} // namespace DB::Etcd
