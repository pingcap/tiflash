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
    static Context getContext(const DB::Settings &settings = DB::Settings())
    {
        static Context context = DB::Context::createGlobal();
        context.getSettingsRef() = settings;
        return context;
    }
};

} // namespace tests
} // namespace DB
