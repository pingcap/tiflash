#pragma once

#include <Core/Types.h>
#include <type_traits>

union PFMetaHeader {
    struct
    {
        UInt32 meta_byte_size : 32;

        UInt32 page_checksum : 32;

        UInt64 meta_seq_id : 64;
    } bits;
    UInt8 raws[16];
};

static_assert(sizeof(union PFMetaHeader) == 16, "Invaild size of PFMetaHeader.");

union PFMeta {

    struct
    {
        // It can support 2^4 - 1 = 15.
        UInt64 type : 4;

        // If single write batch size is 18 * 8.
        // Then log2(16T / (18 * 8))
        // Actually 37 is enough
        UInt64 page_id : 60;

        // The page offset, it record data offset in data buffer
        // The max offset is 2 ^ 44 = 16T. Same as ext4 single file limit.
        // Actually 44 is enough
        UInt64 page_offset : 64;

        // The page size, records data size
        // The max size is 2 ^ 38 - 1 = 256G - 1. Large enough
        UInt64 page_size : 38;

        UInt64 field_offset_len : 26;
    } pu;

    struct 
    {
        UInt64 type : 4;

        UInt64 page_id : 60;

        // unused lane
        UInt64 page_offset : 44;

        UInt64 page_size : 38;

        UInt64 unused : 26;
    } del;

    struct 
    {
        UInt64 type : 4;

        UInt64 page_id : 60;

        UInt64 og_page_id : 64;

        // unused lane

    } ref;
    
    UInt8 raws[24];
};

static_assert(sizeof(union PFMeta) == 24, "Invaild size of PFMeta.");

// field offset
union PFMetaFieldOffset
{
    struct
    {
        // The field offset
        UInt64 field_offset;

    } bits;
    UInt8 raws[8];
};

static_assert(sizeof(union PFMetaFieldOffset) == 8, "Invaild size of PFMetaFieldOffset.");
