#pragma once

#include <gtest/gtest.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace tests
{

class TiFlashTestEnv
{
public:
    static Context getContext()
    {
        static Context context = DB::Context::createGlobal();
        return context;
    }
};

} // namespace tests
} // namespace DB
