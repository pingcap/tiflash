#include <Encryption/AESCTRCipherStream.h>
#include <Encryption/FileProvider.h>
#include <Encryption/MockKeyManager.h>
#include <Encryption/PosixWritableFile.h>
#include <Storages/Transaction/FileEncryption.h>
#include <gtest/gtest.h>
#include <TestUtils/TiFlashTestBasic.h>

#include <random>

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

        int ret = 1;
        EVP_CIPHER_CTX * ctx;
        InitCipherContext(ctx);
        assert(ctx != nullptr);

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
                assert(false);
        }
        assert(cipher != nullptr);

        ret = EVP_EncryptInit(ctx, cipher, test::KEY, iv);
        assert(ret == 1);
        int output_size = 0;
        ret = EVP_EncryptUpdate(ctx, ciphertext, &output_size, plaintext, static_cast<int>(MAX_SIZE));
        assert(ret == 1);
        int final_output_size = 0;
        ret = EVP_EncryptFinal_ex(ctx, ciphertext + output_size, &final_output_size);
        assert(ret == 1);
        assert(output_size + final_output_size == MAX_SIZE);
        FreeCipherContext(ctx);
    }

    void TestEncryptionImpl(size_t start, size_t end, const unsigned char * iv, bool * success)
    {
        assert(start < end && end <= MAX_SIZE);
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

    bool TestEncryption(size_t start, size_t end, const unsigned char * iv = test::IV_RANDOM)
    {
        // Workaround failure of ASSERT_* result in return immediately.
        bool success = false;
        TestEncryptionImpl(start, end, iv, &success);
        return success;
    }
};

TEST_P(EncryptionTest, EncryptionTest)
{
    // One full block.
    EXPECT_TRUE(TestEncryption(0, 16));
    // One block in the middle.
    EXPECT_TRUE(TestEncryption(16 * 5, 16 * 6));
    // Multiple aligned blocks.
    EXPECT_TRUE(TestEncryption(16 * 5, 16 * 8));

    // Random byte at the beginning of a block.
    EXPECT_TRUE(TestEncryption(16 * 5, 16 * 5 + 1));
    // Random byte in the middle of a block.
    EXPECT_TRUE(TestEncryption(16 * 5 + 4, 16 * 5 + 5));
    // Random byte at the end of a block.
    EXPECT_TRUE(TestEncryption(16 * 5 + 15, 16 * 6));

    // Partial block aligned at the beginning.
    EXPECT_TRUE(TestEncryption(16 * 5, 16 * 5 + 15));
    // Partial block aligned at the end.
    EXPECT_TRUE(TestEncryption(16 * 5 + 1, 16 * 6));
    // Multiple blocks with a partial block at the end.
    EXPECT_TRUE(TestEncryption(16 * 5, 16 * 8 + 15));
    // Multiple blocks with a partial block at the beginning.
    EXPECT_TRUE(TestEncryption(16 * 5 + 1, 16 * 8));
    // Partial block at both ends.
    EXPECT_TRUE(TestEncryption(16 * 5 + 1, 16 * 8 + 15));

    // Lower bits of IV overflow.
    EXPECT_TRUE(TestEncryption(16, 16 * 2, test::IV_OVERFLOW_LOW));
    // Full IV overflow.
    EXPECT_TRUE(TestEncryption(16, 16 * 2, test::IV_OVERFLOW_FULL));
}

INSTANTIATE_TEST_CASE_P(EncryptionTestInstance, EncryptionTest,
    testing::Combine(
        testing::Bool(), testing::Values(EncryptionMethod::Aes128Ctr, EncryptionMethod::Aes192Ctr, EncryptionMethod::Aes256Ctr)));


TEST(PosixWritableFile_test, test)
try
{
    String p = tests::TiFlashTestEnv::getTemporaryPath() + "posix_file";
    PosixWritableFile f(p, true, -1, 0600, nullptr);
    f.close();
    f.open();
    f.close();
}
CATCH

} // namespace DB
