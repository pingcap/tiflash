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

#include <Interpreters/Compiler.h>
#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>


int main(int, char **)
{
    using namespace DB;

    Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");

    /// Check exception handling and catching
    try
    {
        Compiler compiler(".", 1);

        auto lib = compiler.getOrCount(
            "catch_me_if_you_can",
            0,
            "",
            []() -> std::string {
                return "#include <iostream>\n"
                       "void f() __attribute__((__visibility__(\"default\")));\n"
                       "void f()"
                       "{"
                       "try { throw std::runtime_error(\"Catch me if you can\"); }"
                       "catch (const std::runtime_error & e) { std::cout << \"Caught in .so: \" << e.what() << std::endl; throw; }\n"
                       "}";
            },
            [](SharedLibraryPtr &) {});

        auto f = lib->template get<void (*)()>("_Z1fv");

        try
        {
            f();
        }
        catch (const std::exception & e)
        {
            std::cout << "Caught in main(): " << e.what() << "\n";
            return 0;
        }
        catch (...)
        {
            std::cout << "Unknown exception\n";
            return -1;
        }
    }
    catch (...)
    {
        std::cerr << getCurrentExceptionMessage(true) << "\n";
        return -1;
    }

    return 0;
}
