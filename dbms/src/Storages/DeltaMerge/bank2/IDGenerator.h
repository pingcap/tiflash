//
// Created by linkmyth on 2020-03-17.
//

#ifndef CLICKHOUSE_IDGENERATOR_H
#define CLICKHOUSE_IDGENERATOR_H
#include <Core/Types.h>
#include <mutex>

namespace DB {
    namespace DM {
        namespace tests {
            class IDGenerator {
            public:
                IDGenerator(): id{0} {

                }

                UInt64 get() {
                    std::lock_guard<std::mutex> guard{mutex};
                    return id++;
                }

            private:
                std::mutex mutex;
                UInt64 id;
            };
        }
    }
}

#endif //CLICKHOUSE_IDGENERATOR_H
