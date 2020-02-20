#pragma once

#include <functional>

#include <Storages/Transaction/PDTiKVClient.h>

namespace DB
{

using IndexReaderCreateFunc = std::function<IndexReaderPtr()>;

} // namespace DB
