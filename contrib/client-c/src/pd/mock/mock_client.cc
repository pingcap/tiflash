#include <pd/Client.h>

int main(int argv, char ** args) {
    std::vector<std::string> addrs;
    for (int i = 1; i < argv; i++)
    {
        addrs.push_back(args[i]);
    }
    pingcap::pd::Client clt(addrs);
    std::string cmd;
    while(std::cin >> cmd) {
        switch(cmd[0]) {
            case 'g':
            {
                auto safe = clt.getGCSafePoint();
                break;
            }
            default:
                throw "unknown cmd: " + cmd[0];
        }
    }
    return 0;
}
