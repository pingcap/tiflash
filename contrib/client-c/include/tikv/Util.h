#pragma once

#include <string>

namespace pingcap {
namespace kv {

inline std::string prefixNext(std::string str) {
    auto new_str = str;
    for (int i = int(str.size()) ; i >= 0 ; i--) {
        char & c = new_str[i-1];
        c ++;
        if (c != 0) {
            return new_str;
        }
    }
    return str + "\0";
}

}
}
