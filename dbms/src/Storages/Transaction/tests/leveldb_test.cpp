#include <Poco/File.h>
#include <leveldb/db.h>
#include <cassert>
#include <ext/scope_guard.h>
#include <iostream>

int main()
{
    std::string path = "/tmp/testdb";
    SCOPE_EXIT({ Poco::File(path).remove(true); });

    leveldb::DB * db;
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, path, &db);
    assert(status.ok());
    std::cerr << status.ToString() << std::endl;

    std::string value;
    leveldb::Status s = db->Put(leveldb::WriteOptions(), "key1", "value");
    if (s.ok())
        s = db->Get(leveldb::ReadOptions(), "key1", &value);
    if (s.ok())
        s = db->Put(leveldb::WriteOptions(), "key2", value);
    if (s.ok())
        s = db->Delete(leveldb::WriteOptions(), "key1");

	std::cerr << status.ToString() << std::endl;

	delete db;
	return 0;
}