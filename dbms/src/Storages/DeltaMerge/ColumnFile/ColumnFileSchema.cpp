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

#include <Storages/DeltaMerge/ColumnFile/ColumnFileSchema.h>


namespace DB
{
namespace DM
{

Digest calcDigest(const Block & schema)
{
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    unsigned char digest_bytes[32];

    const auto & data = schema.getColumnsWithTypeAndName();
    for (const auto & column_with_type_and_name : data)
    {
        const auto & type = *(column_with_type_and_name.type);
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(&type), sizeof(type));

        const auto & name = column_with_type_and_name.name.c_str();
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(name), strlen(name));

        const auto & column_id = column_with_type_and_name.column_id;
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(&column_id), sizeof(column_id));

        const auto & default_value = column_with_type_and_name.default_value.toString();
        const auto & default_value_c_str = default_value.c_str();
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(default_value_c_str), strlen(default_value_c_str));
    }

    SHA256_Final(digest_bytes, &ctx);

    Digest digest;
    digest.a = *(reinterpret_cast<uint64_t *>(digest_bytes));
    digest.b = *(reinterpret_cast<uint64_t *>(digest_bytes + 8));
    digest.c = *(reinterpret_cast<uint64_t *>(digest_bytes + 16));
    digest.d = *(reinterpret_cast<uint64_t *>(digest_bytes + 24));

    return digest;
}
} // namespace DM
} // namespace DB