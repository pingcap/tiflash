#pragma once

#include "Common.h"

extern "C" {

namespace DB
{

enum class FileEncryptionRes : uint8_t
{
    Disabled = 0,
    Ok,
    Error,
};

enum class EncryptionMethod : uint8_t
{
    Unknown = 0,
    Plaintext,
    Aes128Ctr,
    Aes192Ctr,
    Aes256Ctr,
};
struct FileEncryptionInfoRaw
{
    FileEncryptionRes res;
    EncryptionMethod method;
    RawVoidPtr key;
    RawVoidPtr iv;
    RawVoidPtr erro_msg;
};
} // namespace DB
}
