#pragma once

#include <Core/Defines.h>
#include <Core/Types.h>

namespace DB
{

struct NPage
{
public:
    struct FieldOffset
    {


    };

    UInt64 page_id;
    std::set<FieldOffset> field_offsets;
};


struct PageEntry
{
    UInt64 file_id;
    UInt64 page_size;

    UInt64 offset = 0; // Page data's offset in PageFile
    UInt64 tag = 0;
    UInt64 checksum = 0; // The checksum of whole page data
    UInt32 level = 0; // PageFile level
    UInt32 ref = 1; // for ref counting


}

}