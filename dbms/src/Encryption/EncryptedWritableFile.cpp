// Copyright 2023 PingCAP, Inc.
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

#include <Encryption/EncryptedWritableFile.h>

namespace DB
{
void EncryptedWritableFile::open()
{
    file->open();
}

void EncryptedWritableFile::close()
{
    file->close();
}

ssize_t EncryptedWritableFile::write(char * buf, size_t size)
{
    stream->encrypt(file_offset, buf, size);
    file_offset += size;
    return file->write(buf, size);
}

ssize_t EncryptedWritableFile::pwrite(char * buf, size_t size, off_t offset) const
{
    stream->encrypt(offset, buf, size);
    return file->pwrite(buf, size, offset);
}

} // namespace DB
