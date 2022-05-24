// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <Encryption/AESCTRCipherStream.h>
#include <Encryption/EncryptedRandomAccessFile.h>
#include <Encryption/EncryptedWritableFile.h>
#include <Encryption/EncryptedWriteReadableFile.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Encryption/PosixRandomAccessFile.h>
#include <Encryption/PosixWritableFile.h>
#include <Encryption/PosixWriteReadableFile.h>
#include <Encryption/WriteReadableFile.h>
#include <Storages/Transaction/FileEncryption.h>
#include <TestUtils/TiFlashTestBasic.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <random>

#ifdef NDEBUG
#define DBMS_ASSERT(X)    \
    {                     \
        if (!(X))         \
            std::abort(); \
    }
#else
#define DBMS_ASSERT assert
#endif

namespace DB
{
namespace test
{
const unsigned char KEY[33] = "\xe4\x3e\x8e\xca\x2a\x83\xe1\x88\xfb\xd8\x02\xdc\xf3\x62\x65\x3e"
                              "\x00\xee\x31\x39\xe7\xfd\x1d\x92\x20\xb1\x62\xae\xb2\xaf\x0f\x1a";
const unsigned char IV_RANDOM[17] = "\x77\x9b\x82\x72\x26\xb5\x76\x50\xf7\x05\xd2\xd6\xb8\xaa\xa9\x2c";
const unsigned char IV_OVERFLOW_LOW[17] = "\x77\x9b\x82\x72\x26\xb5\x76\x50\xff\xff\xff\xff\xff\xff\xff\xff";
const unsigned char IV_OVERFLOW_FULL[17] = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";

std::string random_string(size_t length)
{
    std::string str("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");
    while (str.length() < length)
    {
        str += str;
    }
    std::random_device rd;
    std::mt19937 generator(rd());
    std::shuffle(str.begin(), str.end(), generator);
    return str.substr(0, length);
}
} // namespace test

constexpr size_t MAX_SIZE = 16 * 10;

// Test to make sure the output of AESCTRCipherStream is the same as output from
// OpenSSL EVP API.
class EncryptionTest : public testing::TestWithParam<std::tuple<bool, EncryptionMethod>>
{
public:
    unsigned char plaintext[MAX_SIZE];
    // Reserve a bit more room to make sure OpenSSL have enough buffer.
    unsigned char ciphertext[MAX_SIZE + 16 * 2];

    void generateCiphertext(const unsigned char * iv)
    {
        std::string random_string = test::random_string(MAX_SIZE);
        memcpy(plaintext, random_string.data(), MAX_SIZE);

        EVP_CIPHER_CTX * ctx;
        InitCipherContext(ctx);
        DBMS_ASSERT(ctx != nullptr);

        const EVP_CIPHER * cipher = nullptr;
        EncryptionMethod method = std::get<1>(GetParam());
        switch (method)
        {
        case EncryptionMethod::Aes128Ctr:
            cipher = EVP_aes_128_ctr();
            break;
        case EncryptionMethod::Aes192Ctr:
            cipher = EVP_aes_192_ctr();
            break;
        case EncryptionMethod::Aes256Ctr:
            cipher = EVP_aes_256_ctr();
            break;
        default:
            DBMS_ASSERT(false);
        }
        DBMS_ASSERT(cipher != nullptr);

        int ret = EVP_EncryptInit(ctx, cipher, test::KEY, iv);
        DBMS_ASSERT(ret == 1);
        int output_size = 0;
        ret = EVP_EncryptUpdate(ctx, ciphertext, &output_size, plaintext, static_cast<int>(MAX_SIZE));
        DBMS_ASSERT(ret == 1);
        int final_output_size = 0;
        ret = EVP_EncryptFinal_ex(ctx, ciphertext + output_size, &final_output_size);
        DBMS_ASSERT(ret == 1);
        DBMS_ASSERT(output_size + final_output_size == MAX_SIZE);
        FreeCipherContext(ctx);
    }

    void testEncryptionImpl(size_t start, size_t end, const unsigned char * iv, bool * success)
    {
        DBMS_ASSERT(start < end && end <= MAX_SIZE);
        generateCiphertext(iv);

        EncryptionMethod method = std::get<1>(GetParam());
        std::string key_str(reinterpret_cast<const char *>(test::KEY), KeySize(method));
        std::string iv_str(reinterpret_cast<const char *>(iv), 16);
        KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(method, key_str, iv_str);
        auto encryption_info = key_manager->newFile("encryption");
        BlockAccessCipherStreamPtr cipher_stream
            = AESCTRCipherStream::createCipherStream(encryption_info, EncryptionPath("encryption", ""));

        size_t data_size = end - start;
        // Allocate exact size. AESCTRCipherStream should make sure there will be
        // no memory corruption.
        std::unique_ptr<char[]> data(new char[data_size]);
        if (std::get<0>(GetParam()))
        {
            // Encrypt
            memcpy(data.get(), plaintext + start, data_size);
            cipher_stream->encrypt(start, data.get(), data_size);
            ASSERT_EQ(0, memcmp(ciphertext + start, data.get(), data_size));
        }
        else
        {
            // Decrypt
            memcpy(data.get(), ciphertext + start, data_size);
            cipher_stream->decrypt(start, data.get(), data_size);
            ASSERT_EQ(0, memcmp(plaintext + start, data.get(), data_size));
        }
        *success = true;
    }

    bool testEncryption(size_t start, size_t end, const unsigned char * iv = test::IV_RANDOM)
    {
        // Workaround failure of ASSERT_* result in return immediately.
        bool success = false;
        testEncryptionImpl(start, end, iv, &success);
        return success;
    }
};

TEST_P(EncryptionTest, EncryptionTest)
{
    // One full block.
    EXPECT_TRUE(testEncryption(0, 16));
    // One block in the middle.
    EXPECT_TRUE(testEncryption(16 * 5, 16 * 6));
    // Multiple aligned blocks.
    EXPECT_TRUE(testEncryption(16 * 5, 16 * 8));

    // Random byte at the beginning of a block.
    EXPECT_TRUE(testEncryption(16 * 5, 16 * 5 + 1));
    // Random byte in the middle of a block.
    EXPECT_TRUE(testEncryption(16 * 5 + 4, 16 * 5 + 5));
    // Random byte at the end of a block.
    EXPECT_TRUE(testEncryption(16 * 5 + 15, 16 * 6));

    // Partial block aligned at the beginning.
    EXPECT_TRUE(testEncryption(16 * 5, 16 * 5 + 15));
    // Partial block aligned at the end.
    EXPECT_TRUE(testEncryption(16 * 5 + 1, 16 * 6));
    // Multiple blocks with a partial block at the end.
    EXPECT_TRUE(testEncryption(16 * 5, 16 * 8 + 15));
    // Multiple blocks with a partial block at the beginning.
    EXPECT_TRUE(testEncryption(16 * 5 + 1, 16 * 8));
    // Partial block at both ends.
    EXPECT_TRUE(testEncryption(16 * 5 + 1, 16 * 8 + 15));

    // Lower bits of IV overflow.
    EXPECT_TRUE(testEncryption(16, 16 * 2, test::IV_OVERFLOW_LOW));
    // Full IV overflow.
    EXPECT_TRUE(testEncryption(16, 16 * 2, test::IV_OVERFLOW_FULL));
}

INSTANTIATE_TEST_CASE_P(EncryptionTestInstance, EncryptionTest, testing::Combine(testing::Bool(), testing::Values(EncryptionMethod::Aes128Ctr, EncryptionMethod::Aes192Ctr, EncryptionMethod::Aes256Ctr)));


TEST(PosixWritableFileTest, test)
try
{
    String p = tests::TiFlashTestEnv::getTemporaryPath("posix_file");
    PosixWritableFile f(p, true, -1, 0600, nullptr);
    f.close();
    f.open();
    f.close();
}
CATCH

TEST(PosixWriteReadableFileTest, WriteRead)
try
{
    size_t buff_size = 123;
    size_t buff_offset = 20;
    char buff_write[buff_size];
    char buff_read[buff_size];

    for (size_t i = 0; i < buff_size; i++)
    {
        buff_write[i] = i % 0xFF;
    }

    String file_path = tests::TiFlashTestEnv::getTemporaryPath("posix_wr_file");
    WriteReadableFilePtr file = std::make_shared<PosixWriteReadableFile>(file_path, true, -1, 0600, nullptr, nullptr);

    ASSERT_EQ(buff_size, file->pwrite(buff_write, buff_size, buff_offset));
    ASSERT_EQ(buff_size, file->pread(buff_read, buff_size, buff_offset));
    ASSERT_EQ(strncmp(buff_write, buff_read, buff_size), 0);

    file->close();
    ASSERT_TRUE(file->isClosed());

    // Do it twice to ensure we can call close safely on a closed file
    file->close();
    ASSERT_TRUE(file->isClosed());
}
CATCH


TEST(PosixWriteReadableFileTest, WriteReadwithFileProvider)
try
{
    size_t buff_size = 123;
    size_t buff_offset = 20;
    char buff_write[buff_size];
    char buff_read[buff_size];

    for (size_t i = 0; i < buff_size; i++)
    {
        buff_write[i] = i % 0xFF;
    }

    String file_path = tests::TiFlashTestEnv::getTemporaryPath("posix_wr_file");

    auto key_manager = std::make_shared<MockKeyManager>();
    auto file_provider = std::make_shared<FileProvider>(key_manager, false);

    WriteReadableFilePtr file = file_provider->newWriteReadableFile(file_path, EncryptionPath("encryption", ""));

    ASSERT_EQ(buff_size, file->pwrite(buff_write, buff_size, buff_offset));
    ASSERT_EQ(buff_size, file->pread(buff_read, buff_size, buff_offset));
    ASSERT_EQ(strncmp(buff_write, buff_read, buff_size), 0);

    file->close();
    ASSERT_TRUE(file->isClosed());

    // Do it twice to ensure we can call close safely on a closed file
    file->close();
    ASSERT_TRUE(file->isClosed());
}
CATCH

TEST(PosixWriteReadableFileTest, EncWriteReadwithFileProvider)
try
{
    size_t buff_size = 123;
    size_t buff_offset = 20;
    char buff_write[buff_size];
    char buff_read[buff_size];
    char buff_write_cpy[buff_size];

    for (size_t i = 0; i < buff_size; i++)
    {
        buff_write[i] = i % 0xFF;
    }

    memcpy(buff_write_cpy, buff_write, buff_size);

    String file_path = tests::TiFlashTestEnv::getTemporaryPath("enc_posix_wr_file");

    std::string key_str(reinterpret_cast<const char *>(test::KEY), KeySize(EncryptionMethod::Aes128Ctr));
    std::string iv_str(reinterpret_cast<const char *>(test::IV_RANDOM), 16);
    KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(EncryptionMethod::Aes128Ctr, key_str, iv_str);
    auto file_provider = std::make_shared<FileProvider>(key_manager, true);

    WriteReadableFilePtr file = file_provider->newWriteReadableFile(file_path, EncryptionPath("encryption", ""));

    ASSERT_EQ(buff_size, file->pwrite(buff_write, buff_size, buff_offset));
    ASSERT_EQ(buff_size, file->pread(buff_read, buff_size, buff_offset));
    ASSERT_EQ(strncmp(buff_write_cpy, buff_read, buff_size), 0);

    file->close();
    ASSERT_TRUE(file->isClosed());

    // Do it twice to ensure we can call close safely on a closed file
    file->close();
    ASSERT_TRUE(file->isClosed());
}
CATCH

TEST(PosixWriteReadableFileTest, EncryptedWriteRead)
try
{
    String file_path = tests::TiFlashTestEnv::getTemporaryPath("enc_posix_wr_file");
    WriteReadableFilePtr file = std::make_shared<PosixWriteReadableFile>(file_path, true, -1, 0600, nullptr, nullptr);

    std::string key_str(reinterpret_cast<const char *>(test::KEY), KeySize(EncryptionMethod::Aes128Ctr));
    std::string iv_str(reinterpret_cast<const char *>(test::IV_RANDOM), 16);
    KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(EncryptionMethod::Aes128Ctr, key_str, iv_str);
    auto encryption_info = key_manager->newFile("encryption");
    BlockAccessCipherStreamPtr cipher_stream
        = AESCTRCipherStream::createCipherStream(encryption_info, EncryptionPath("encryption", ""));

    WriteReadableFilePtr enc_file = std::make_shared<EncryptedWriteReadableFile>(file, cipher_stream);

    size_t buff_size = 123;
    size_t buff_offset = 20;
    char buff_write[buff_size];
    char buff_read[buff_size];
    char buff_write_cpy[buff_size];

    for (size_t i = 0; i < buff_size; i++)
    {
        buff_write[i] = i % 0xFF;
    }
    memcpy(buff_write_cpy, buff_write, buff_size);

    ASSERT_EQ(buff_size, enc_file->pwrite(buff_write, buff_size, buff_offset));
    ASSERT_EQ(buff_size, enc_file->pread(buff_read, buff_size, buff_offset));
    ASSERT_EQ(strncmp(buff_write_cpy, buff_read, buff_size), 0);

    enc_file->close();
    ASSERT_TRUE(enc_file->isClosed());

    // Do it twice to ensure we can call close safely on a closed file
    enc_file->close();
    ASSERT_TRUE(enc_file->isClosed());
}
CATCH

class FtruncateTest : public ::testing::Test
{
public:
    template <typename B, typename T, typename E>
    void testEncFtruncate(String file_name)
    {
        String file_path = tests::TiFlashTestEnv::getTemporaryPath(file_name);
        B file = std::make_shared<T>(file_path, true, -1, 0600);

        std::string key_str(reinterpret_cast<const char *>(test::KEY), KeySize(EncryptionMethod::Aes128Ctr));
        std::string iv_str(reinterpret_cast<const char *>(test::IV_RANDOM), 16);
        KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(EncryptionMethod::Aes128Ctr, key_str, iv_str);
        auto encryption_info = key_manager->newFile("encryption");
        BlockAccessCipherStreamPtr cipher_stream
            = AESCTRCipherStream::createCipherStream(encryption_info, EncryptionPath("encryption", ""));

        auto enc_file = std::make_shared<E>(file, cipher_stream);

        size_t buff_size = 123;
        size_t buff_need_delete_size = 23;
        char buff_write[buff_size];

        size_t truncate_size = buff_size - buff_need_delete_size;

        for (size_t i = 0; i < buff_size; i++)
        {
            buff_write[i] = i % 0xFF;
        }

        ASSERT_EQ(buff_size, file->pwrite(buff_write, buff_size, 0));
        ASSERT_EQ(enc_file->fsync(), 0);
        ASSERT_EQ(enc_file->ftruncate(truncate_size), 0);
        ASSERT_EQ(Poco::File(file_path).getSize(), truncate_size);

        enc_file->close();
        ASSERT_TRUE(enc_file->isClosed());
    }

    template <typename T>
    void testFtruncate(String file_name)
    {
        size_t buff_size = 123;
        size_t buff_need_delete_size = 23;
        char buff_write[buff_size];

        size_t truncate_size = buff_size - buff_need_delete_size;

        for (size_t i = 0; i < buff_size; i++)
        {
            buff_write[i] = i % 0xFF;
        }

        String file_path = tests::TiFlashTestEnv::getTemporaryPath(file_name);
        auto file = std::make_shared<T>(file_path, true, -1, 0600);

        ASSERT_EQ(buff_size, file->pwrite(buff_write, buff_size, 0));
        ASSERT_EQ(file->fsync(), 0);
        ASSERT_EQ(file->ftruncate(truncate_size), 0);
        ASSERT_EQ(Poco::File(file_path).getSize(), truncate_size);

        file->close();
        ASSERT_TRUE(file->isClosed());
    }
};

TEST_F(FtruncateTest, Ftruncate)
try
{
    testFtruncate<PosixWritableFile>("posix_w_file");
    testFtruncate<PosixWriteReadableFile>("posix_wr_file");
    testEncFtruncate<WritableFilePtr, PosixWritableFile, EncryptedWritableFile>("enc_posix_w_file");
    testEncFtruncate<WriteReadableFilePtr, PosixWriteReadableFile, EncryptedWriteReadableFile>("enc_posix_wr_file");
}
CATCH

TEST(PosixWritableFileTest, hardlink)
try
{
    size_t buff_size = 123;
    char buff_write[buff_size];

    for (size_t i = 0; i < buff_size; i++)
    {
        buff_write[i] = i % 0xFF;
    }

    String file_path = tests::TiFlashTestEnv::getTemporaryPath("posix_file");
    PosixWritableFile file(file_path, true, -1, 0600, nullptr);
    file.write(buff_write, buff_size);
    file.close();

    String linked_file_path = tests::TiFlashTestEnv::getTemporaryPath("posix_linked_file");
    PosixWritableFile linked_file(linked_file_path, true, -1, 0600, nullptr);
    linked_file.hardLink(file_path);
    linked_file.close();

    // Check the stat
    struct stat file_stat;
    ASSERT_EQ(0, stat(linked_file_path.c_str(), &file_stat));
    ASSERT_EQ(2, file_stat.st_nlink);

    // Remove the origin file
    auto origin_file = Poco::File(file_path);
    ASSERT_TRUE(origin_file.exists());
    origin_file.remove();

    // Read and check
    char buff_read[buff_size];
    RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(linked_file_path, -1, nullptr);
    file_for_read->read(buff_read, buff_size);
    file_for_read->close();
    ASSERT_EQ(strncmp(buff_write, buff_read, buff_size), 0);
}
CATCH

TEST(PosixWritableFileTest, hardlinkEnc)
try
{
    String file_path = tests::TiFlashTestEnv::getTemporaryPath("enc_posix_file");
    WritableFilePtr file = std::make_shared<PosixWritableFile>(file_path, true, -1, 0600, nullptr);

    std::string key_str(reinterpret_cast<const char *>(test::KEY), KeySize(EncryptionMethod::Aes128Ctr));
    std::string iv_str(reinterpret_cast<const char *>(test::IV_RANDOM), 16);
    KeyManagerPtr key_manager = std::make_shared<MockKeyManager>(EncryptionMethod::Aes128Ctr, key_str, iv_str);
    auto encryption_info = key_manager->newFile("encryption");
    BlockAccessCipherStreamPtr cipher_stream
        = AESCTRCipherStream::createCipherStream(encryption_info, EncryptionPath("encryption", ""));

    EncryptedWritableFile enc_file(file, cipher_stream);

    size_t buff_size = 123;
    char buff_write[buff_size];

    for (size_t i = 0; i < buff_size; i++)
    {
        buff_write[i] = i % 0xFF;
    }

    char buff_write_cpy[buff_size];
    memcpy(buff_write_cpy, buff_write, buff_size);

    enc_file.write(buff_write_cpy, buff_size);
    enc_file.fsync();
    enc_file.close();

    String linked_file_path = tests::TiFlashTestEnv::getTemporaryPath("enc_linked_posix_file");
    WritableFilePtr linked_file = std::make_shared<PosixWritableFile>(linked_file_path, true, -1, 0600, nullptr);
    EncryptedWritableFile linked_enc_file(linked_file, cipher_stream);

    linked_enc_file.hardLink(file_path);
    linked_enc_file.close();

    // Check the stat
    struct stat file_stat;
    ASSERT_EQ(0, stat(linked_file_path.c_str(), &file_stat));
    ASSERT_EQ(2, file_stat.st_nlink);

    // Remove the origin file
    auto origin_file = Poco::File(file_path);
    ASSERT_TRUE(origin_file.exists());
    origin_file.remove();

    // Read and check
    char buff_read[buff_size];
    RandomAccessFilePtr file_for_read = std::make_shared<PosixRandomAccessFile>(linked_file_path, -1, nullptr);
    EncryptedRandomAccessFile enc_file_for_read(file_for_read, cipher_stream);
    enc_file_for_read.read(buff_read, buff_size);
    enc_file_for_read.close();

    ASSERT_EQ(strncmp(buff_write, buff_read, buff_size), 0);
}
CATCH

} // namespace DB
