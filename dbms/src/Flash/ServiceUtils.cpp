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

#include <Core/Field.h>
#include <Flash/ServiceUtils.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_USER;
extern const int WRONG_PASSWORD;
extern const int REQUIRED_PASSWORD;
extern const int IP_ADDRESS_NOT_ALLOWED;
} // namespace ErrorCodes

grpc::StatusCode tiflashErrorCodeToGrpcStatusCode(int error_code)
{
    /// do not use switch statement because ErrorCodes::XXXX is not a compile time constant
    if (error_code == ErrorCodes::NOT_IMPLEMENTED)
        return grpc::StatusCode::UNIMPLEMENTED;
    if (error_code == ErrorCodes::UNKNOWN_USER || error_code == ErrorCodes::WRONG_PASSWORD
        || error_code == ErrorCodes::REQUIRED_PASSWORD || error_code == ErrorCodes::IP_ADDRESS_NOT_ALLOWED)
        return grpc::StatusCode::UNAUTHENTICATED;
    return grpc::StatusCode::INTERNAL;
}

} // namespace DB