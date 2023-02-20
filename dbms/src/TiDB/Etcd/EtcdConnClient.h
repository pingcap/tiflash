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
