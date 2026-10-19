// SPDX-License-Identifier: MIT
// Portable SIMD byte scanning used by the feed parser.
//
// The JSON parser spends most of its time finding quotes, commas and colons.
// Scanning 32 (AVX2) or 16 (NEON/SSE2) bytes per iteration removes the
// branch-per-byte cost of a naive parser. The fallback uses libc memchr which
// is itself SIMD-optimised on every supported platform.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>

#if defined(__ARM_NEON)
#include <arm_neon.h>
#elif defined(__AVX2__)
#include <immintrin.h>
#elif defined(__SSE2__)
#include <emmintrin.h>
#endif

namespace hft::simd {

inline const char* find_char(const char* begin, const char* end, char needle) noexcept {
    const auto* p = reinterpret_cast<const std::uint8_t*>(begin);
    const auto* e = reinterpret_cast<const std::uint8_t*>(end);

#if defined(__ARM_NEON)
    const uint8x16_t target = vdupq_n_u8(static_cast<std::uint8_t>(needle));
    while (p + 16 <= e) {
        const uint8x16_t block = vld1q_u8(p);
        const uint8x16_t equal = vceqq_u8(block, target);
        // Compress 16 byte-lanes into 16 nibble flags, then ctz/4 -> byte index.
        const uint8x8_t narrowed = vshrn_n_u16(vreinterpretq_u16_u8(equal), 4);
        const std::uint64_t mask = vget_lane_u64(vreinterpret_u64_u8(narrowed), 0);
        if (mask != 0) {
            return reinterpret_cast<const char*>(p + (__builtin_ctzll(mask) >> 2));
        }
        p += 16;
    }
#elif defined(__AVX2__)
    const __m256i target = _mm256_set1_epi8(needle);
    while (p + 32 <= e) {
        const __m256i block = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
        const std::uint32_t mask = static_cast<std::uint32_t>(
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, target)));
        if (mask != 0) {
            return reinterpret_cast<const char*>(p + __builtin_ctz(mask));
        }
        p += 32;
    }
#elif defined(__SSE2__)
    const __m128i target = _mm_set1_epi8(needle);
    while (p + 16 <= e) {
        const __m128i block = _mm_loadu_si128(reinterpret_cast<const __m128i*>(p));
        const std::uint32_t mask =
            static_cast<std::uint32_t>(_mm_movemask_epi8(_mm_cmpeq_epi8(block, target)));
        if (mask != 0) {
            return reinterpret_cast<const char*>(p + __builtin_ctz(mask));
        }
        p += 16;
    }
#endif

    const void* hit = std::memchr(p, static_cast<unsigned char>(needle),
                                  static_cast<std::size_t>(e - p));
    return hit != nullptr ? static_cast<const char*>(hit) : end;
}

inline const char* skip_whitespace(const char* begin, const char* end) noexcept {
    while (begin < end) {
        const char c = *begin;
        if (c != ' ' && c != '\t' && c != '\n' && c != '\r') {
            break;
        }
        ++begin;
    }
    return begin;
}

// Parses an unsigned integer without allocation. Returns false on malformed
// input or overflow.
inline bool parse_uint64(const char* begin, const char* end, std::uint64_t& out) noexcept {
    if (begin == nullptr || begin == end) {
        return false;
    }
    std::uint64_t value = 0;
    for (const char* p = begin; p < end; ++p) {
        if (*p < '0' || *p > '9') {
            return false;
        }
        const std::uint64_t digit = static_cast<std::uint64_t>(*p - '0');
        if (value > (UINT64_MAX - digit) / 10U) {
            return false;
        }
        value = value * 10U + digit;
    }
    out = value;
    return true;
}

}  // namespace hft::simd
