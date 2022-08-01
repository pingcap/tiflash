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

std::atomic<long long> tracked_proto{0};
namespace DB
{
 void TrackedMppDataPacket::alloc()
    {
        if (size)
        {
            try
            {
                // TODO: troubleshoot what cause CurrentMemoryTracker incorrect
                // server.context().getGlobalContext().getProcessList().total_memory_tracker.alloc(size);
                //TrackedMppDataPacket::memory_tracker->alloc(size)
                if (memory_tracker) {
                    memory_tracker->alloc(size);
                //     current_memory_tracker->alloc(size);


                    tracked_proto += size;
                    //update track mem
                    tracked_mem += size;
                    long long cur_mem = tracked_mem;
                    if (cur_mem > tracked_peak) {
                        tracked_peak = cur_mem;
                    }
                }
                //CurrentMemoryTracker::alloc(size);
                
            }
            catch (...)
            {
                has_err = true;
                // tracked_proto -= size;
                std::rethrow_exception(std::current_exception());
            }
        }
    }

    void TrackedMppDataPacket::trackFree() const
    {
        if (size && !has_err)
        {
            // TODO: troubleshoot what cause CurrentMemoryTracker incorrect
            if (memory_tracker) {
                memory_tracker->free(size);
                //     current_memory_tracker->free(size);

                tracked_proto -= size;
                tracked_mem -= size;
            }
            // CurrentMemoryTracker::free(size);
            
        }
    }

}