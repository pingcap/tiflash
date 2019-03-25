#include <tikv/Rpc.h>

namespace pingcap {
namespace kv {

ConnArray::ConnArray(size_t max_size, std::string addr) : index(0) {
    vec.resize(max_size);
    for (size_t i = 0; i < max_size; i++) {
        vec[i] = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    }
}

std::shared_ptr<grpc::Channel> ConnArray::get() {
    std::lock_guard<std::mutex> lock(mutex);
    index = (index + 1) % vec.size();
    return vec[index];
}

ConnArrayPtr RpcClient::getConnArray(const std::string & addr) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = conns.find(addr);
    if (it == conns.end())
    {
        return createConnArray(addr);
    }
    return it->second;
}

ConnArrayPtr RpcClient::createConnArray(const std::string & addr) {
    auto conn_array = std::make_shared<ConnArray>(5, addr);
    conns[addr] = conn_array;
    return conn_array;
}

}
}
