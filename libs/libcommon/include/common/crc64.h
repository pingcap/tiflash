#pragma once
#include <cstdint>
#include <cstddef>
namespace crc64
{
enum class Mode
{
    Table,
    Auto,
    SIMD_128,
    SIMD_256, // always define this, but no effect if VPCLMULQDQ support is unavailable
    SIMD_512, // always define this, but no effect if VPCLMULQDQ support is unavailable
};
class Digest
{
public:
    explicit Digest(Mode mode = Mode::Auto);

    void update(const void * src, size_t length) { state = update_fn(state, src, length); }

    [[nodiscard]] uint64_t checksum() const { return ~state; }

private:
    uint64_t (*update_fn)(uint64_t, const void *, size_t) = nullptr;

    uint64_t state = ~0ull;
};
} // namespace crc64
