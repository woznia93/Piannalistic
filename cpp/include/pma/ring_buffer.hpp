#pragma once
#include <atomic>
#include <cstddef>
#include <memory>
#include <type_traits>

namespace pma {

// Lock-free single-producer / single-consumer queue.
// Exactly one thread may call try_push and exactly one may call try_pop.
// Capacity is rounded up to a power of two so indexing is a mask, not a modulo.
template <typename T>
class SpscRingBuffer {
    static_assert(std::is_trivially_copyable<T>::value,
                  "SpscRingBuffer requires a trivially copyable type");

public:
    explicit SpscRingBuffer(std::size_t min_capacity)
        : capacity_(round_up_pow2(min_capacity)),
          mask_(capacity_ - 1),
          buf_(new T[capacity_]) {}

    bool try_push(const T& item) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head - tail_cache_ == capacity_) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (head - tail_cache_ == capacity_) return false;  // full
        }
        buf_[head & mask_] = item;
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

    bool try_pop(T& out) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_cache_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (tail == head_cache_) return false;  // empty
        }
        out = buf_[tail & mask_];
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    std::size_t capacity() const noexcept { return capacity_; }

    // Approximate when called concurrently with push/pop.
    std::size_t size() const noexcept {
        return head_.load(std::memory_order_acquire) -
               tail_.load(std::memory_order_acquire);
    }

private:
    static std::size_t round_up_pow2(std::size_t n) {
        std::size_t p = 2;
        while (p < n) p <<= 1;
        return p;
    }

    static constexpr std::size_t kCacheLine = 64;

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<T[]> buf_;

    // Producer-owned and consumer-owned state live on separate cache lines
    // so the two threads do not false-share.
    alignas(kCacheLine) std::atomic<std::size_t> head_{0};
    std::size_t tail_cache_ = 0;  // producer's last-seen tail
    alignas(kCacheLine) std::atomic<std::size_t> tail_{0};
    std::size_t head_cache_ = 0;  // consumer's last-seen head
};

}  // namespace pma
