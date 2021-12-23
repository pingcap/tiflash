#include <Storages/DeltaMerge/tools/workload/DTWorkload.h>

using namespace DB::DM::tests;

int main(int argc, char ** argv)
{
    return DTWorkload::mainEntry(argc, argv);
}
