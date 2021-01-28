#include <Storages/Transaction/SSTReader.h>

#include <vector>

namespace DB
{

bool SSTReader::remained() const { return proxy_helper->sst_reader_interfaces.fn_remained(inner, type); }
BaseBuffView SSTReader::key() const { return proxy_helper->sst_reader_interfaces.fn_key(inner, type); }
BaseBuffView SSTReader::value() const { return proxy_helper->sst_reader_interfaces.fn_value(inner, type); }
void SSTReader::next() { return proxy_helper->sst_reader_interfaces.fn_next(inner, type); }

SSTReader::SSTReader(const TiFlashRaftProxyHelper * proxy_helper_, SSTView view)
    : proxy_helper(proxy_helper_),
      inner(proxy_helper->sst_reader_interfaces.fn_get_sst_reader(view, proxy_helper->proxy_ptr)),
      type(view.type)
{}

SSTReader::~SSTReader() { proxy_helper->sst_reader_interfaces.fn_gc(inner, type); }

} // namespace DB
