#pragma once

#include <unordered_map>

namespace DB {

    extern std::unordered_map<tipb::ExprType, String> aggFunMap;
    extern std::unordered_map<tipb::ScalarFuncSig, String> scalarFunMap;

}
