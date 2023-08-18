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

#include <iostream>

#include <Poco/Net/NetException.h>

#include <Common/Exception.h>


int main(int, char **)
{
    try
    {
        //throw Poco::Net::ConnectionRefusedException();
        throw DB::Exception(Poco::Net::ConnectionRefusedException());
    }
    catch (const DB::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << e.displayText() << std::endl;
    }

<<<<<<< HEAD:dbms/src/Core/tests/exception.cpp
    return 0;
}
=======
    static ExecutionResult success() { return {true, nullptr}; }

    static ExecutionResult fail(const std::exception_ptr & exception)
    {
        RUNTIME_CHECK(exception != nullptr);
        return {false, exception};
    }
};
} // namespace DB
>>>>>>> 6638f2067b (Fix license and format coding style (#7962)):dbms/src/Flash/Executor/ExecutionResult.h
