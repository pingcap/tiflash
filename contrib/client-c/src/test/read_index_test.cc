#include <tikv/RegionClient.h>
#include <tikv/Rpc.h>
#include "MockPDServer.h"

namespace pingcap
{
namespace test
{
bool testReadIndex()
{
    std::vector<std::string> addrs;
    for (int i = 1; i <= 3; i++)
    {
        addrs.push_back("127.0.0.1:" + std::to_string(5000 + i));
    }

    PDServerHandler handler(addrs);

    PDService * pd_server = handler.RunPDServer();

    pd_server->addStore();
    pd_server->addStore();
    pd_server->addStore();
    kv::RegionVerID verID(1, 3, 0);
    ::metapb::Region region = generateRegion(verID, "a", "b");
    pd_server->addRegion(region, 0, 1);
    pd_server->stores[1]->setReadIndex(5);

    ::sleep(1);
    pd::ClientPtr clt = std::make_shared<pd::Client>(addrs);
    kv::RegionCachePtr cache = std::make_shared<kv::RegionCache>(clt, "zone", "engine");
    kv::RpcClientPtr rpc = std::make_shared<kv::RpcClient>();
    kv::RegionClient client(cache, rpc, verID);
    int idx = client.getReadIndex();
    if (idx != 5)
    {
        return false;
    }
    return true;
}
} // namespace test
} // namespace pingcap

int main(int argv, char ** args)
{
    if (!pingcap::test::testReadIndex())
    {
        throw "get gc point wrong !";
    }
    return 0;
}
