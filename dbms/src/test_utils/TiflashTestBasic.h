#pragma once

#include <Interpreters/Context.h>
#include <gtest/gtest.h>

namespace DB
{
namespace tests
{

class TiFlashTestEnv
{
public:
    static Context getContext(const DB::Settings & settings = DB::Settings())
    {
        static Context context = DB::Context::createGlobal();
        try
        {
            context.getTMTContext();
        }
        catch (Exception & e)
        {
            context.createTMTContext({}, "", "", {"default"}, "./__tmp_data/kvstore", "./__tmp_data/regmap");
        }
        context.getSettingsRef() = settings;
        return context;
    }
};

} // namespace tests
} // namespace DB
