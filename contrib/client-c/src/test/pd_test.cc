#include <unistd.h>

#include <iostream>
#include <pd/Client.h>
#include "MockPDServer.h"

namespace pingcap {
namespace test{

bool testPDGetGCSafePoint() {
    std::vector<std::string> addrs;
    for (int i = 1; i <= 3; i++)
    {
        addrs.push_back("127.0.0.1:" + std::to_string(5000+i));
    }

    PDServerHandler handler(addrs);

    PDService * pd_server = handler.RunPDServer();

    ::sleep(1);

    pd_server -> setGCPoint(233);

    pd::Client clt(addrs);
    auto safe = clt.getGCSafePoint();

    if (safe != 233) {
        return false;
    }
    std::cout<<"success!!!!!\n";
    return true;
}

}
}

int main(int argv, char** args)
{
    if (!pingcap::test::testPDGetGCSafePoint()) {
        throw "get gc point wrong !";
    }

    return 0;
}

