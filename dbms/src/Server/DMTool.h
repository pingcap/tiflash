#pragma once
#include <string>
#include <vector>
#define _TO_STRING(X) #X
#define TO_STRING(X) _TO_STRING(X)

int mainEntryTiFlashDMTool(int argc, char ** argv);
int benchEntry(const std::vector<std::string> & opts);
int inspectEntry(const std::vector<std::string> & opts);
int migrateEntry(const std::vector<std::string> & opts);