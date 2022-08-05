// Copyright 2022 PingCAP, Ltd.
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

#include "TrackedMppDataPacket.h"
#include <sstream>

std::atomic<long long> tracked_proto{0}, untracked_proto{0};
namespace DB
{
 void TrackedMppDataPacket::alloc()
    {
        if (size)
        {
            try
            {
                if (memory_tracker) {
                    if (!memory_tracker->alloc(size)) {
                        std::stringstream ss;
                        ss<<"[woodyww.alloc]proc_memory_tracker not visited src: "<< src <<", alloc:"<<size;
                        std::cerr<<ss.str()<<std::endl;
                    } 

                    tracked_proto += size;
                    //update track mem
                    tracked_mem += size;
                    long long cur_mem = tracked_mem;
                    if (cur_mem > tracked_peak) {
                        tracked_peak = cur_mem;
                    }
                } else {
                    std::stringstream ss;
                    ss<<"[woodyww.alloc]proc_memory_tracker is null src: "<< src <<", alloc:"<<size;
                    std::cerr<<ss.str()<<std::endl;
                    untracked_proto += size;
                }
                
            }
            catch (...)
            {
                has_err = true;
                // tracked_proto -= size;
                std::rethrow_exception(std::current_exception());
            }
        }
       
    }

    void TrackedMppDataPacket::trackFree() 
    {
        if (size && !has_err)
        {
            if (memory_tracker) {
                // if (memory_tracker->closed) {
                //     std::stringstream ss;
                //     ss<<"memory_tracker has been closed! source: "<<src;
                //     std::cerr<<ss.str()<<std::endl;
                // }
                memory_tracker->free(size);

                tracked_proto -= size;
                tracked_mem -= size;
            } else {
                untracked_proto -= size;
            }
            
        }
    }

}