#pragma once

#include <Encryption/KeyManager.h>

namespace DB {
// TODO: remove KEY and IV_RANDOM here after implement real key manager
const char KEY[17] =
    "\xe4\x3e\x8e\xca\x2a\x83\xe1\x88\xfb\xd8\x02\xdc\xf3\x62\x65\x3e";
const char IV_RANDOM[17] =
    "\x77\x9b\x82\x72\x26\xb5\x76\x50\xf7\x05\xd2\xd6\xb8\xaa\xa9\x2c";

class MockKeyManager : public KeyManager {
public:
  virtual ~MockKeyManager() = default;

  MockKeyManager(): method{EncryptionMethod::kAES128_CTR}, key{std::string(KEY)}, iv{std::string(IV_RANDOM)} {

  }

  MockKeyManager(EncryptionMethod method_, const std::string & key_, const std::string & iv)
  : method{method_}, key{key_}, iv{iv} {

  }

  FileEncryptionInfoPtr getFile(const std::string& fname) override {
        std::ignore = fname;
        auto file_info = std::make_shared<FileEncryptionInfo>();
        file_info->method = method;
        file_info->key = key;
        file_info->iv = iv;
        return file_info;
  }

  FileEncryptionInfoPtr newFile(const std::string& fname) override {
        return getFile(fname);
  }

  void deleteFile(const std::string& fname) override { std::ignore = fname; }

  void linkFile(const std::string& src_fname,
                          const std::string& dst_fname) override {
      std::ignore = src_fname;
      std::ignore = dst_fname;
  }

  void renameFile(const std::string& src_fname,
                            const std::string& dst_fname) override {
      std::ignore = src_fname;
      std::ignore = dst_fname;
  }

private:
    EncryptionMethod method;
  std::string key;
  std::string iv;
};
}
