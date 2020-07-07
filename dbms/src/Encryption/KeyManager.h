#pragma once

#include <cstddef>
#include <string>
#include <memory>

namespace DB
{
enum class EncryptionMethod : int {
  kUnknown = 0,
  kPlaintext = 1,
  kAES128_CTR = 2,
  kAES192_CTR = 3,
  kAES256_CTR = 4,
};

inline size_t KeySize(EncryptionMethod method) {
  switch (method) {
    case EncryptionMethod::kAES128_CTR:
      return 16;
    case EncryptionMethod::kAES192_CTR:
      return 24;
    case EncryptionMethod::kAES256_CTR:
      return 32;
    default:
      return 0;
  };
}

struct FileEncryptionInfo {
  EncryptionMethod method = EncryptionMethod::kUnknown;
  std::string key;
  std::string iv;
};

using FileEncryptionInfoPtr = std::shared_ptr<FileEncryptionInfo>;


class KeyManager
{
public:
  virtual ~KeyManager() = default;

  virtual FileEncryptionInfoPtr getFile(const std::string& fname) = 0;

  virtual FileEncryptionInfoPtr newFile(const std::string& fname) = 0;

  virtual void deleteFile(const std::string& fname) = 0;

  virtual void linkFile(const std::string& src_fname,
                          const std::string& dst_fname) = 0;

  virtual void renameFile(const std::string& src_fname,
                            const std::string& dst_fname) = 0;
};

using KeyManagerPtr = std::shared_ptr<KeyManager>;

}
