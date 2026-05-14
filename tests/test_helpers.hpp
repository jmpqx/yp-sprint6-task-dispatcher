#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "queue/queue.hpp"

namespace test_helpers {

inline dispatcher::queue::Task makeCountingTask(std::atomic<int> &counter) {
    return [ptr = &counter]() { ptr->fetch_add(1, std::memory_order_relaxed); };
}

inline dispatcher::queue::Task makeLatencyTask(std::chrono::milliseconds duration) {
    return [duration]() { std::this_thread::sleep_for(duration); };
}

inline dispatcher::queue::Task makeCallbackTask(std::function<void()> cb) { return cb; }

template <typename QueueT, typename TaskFactory>
    requires std::invocable<TaskFactory, int>
std::vector<std::thread> spawnProducers(QueueT &queue, int producer_count, int tasks_per_thread, TaskFactory factory) {
    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (int p = 0; p < producer_count; ++p) {
        producers.emplace_back([&queue, p, tasks_per_thread, factory]() {
            for (int i = 0; i < tasks_per_thread; ++i) {
                queue.push(factory(p * tasks_per_thread + i));
            }
        });
    }
    return producers;
}

template <typename QueueT>
std::vector<std::thread> spawnConsumers(QueueT &queue, int consumer_count, std::atomic<int> &consumed, int expected,
                                        std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    std::vector<std::thread> consumers;
    consumers.reserve(consumer_count);
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    for (int c = 0; c < consumer_count; ++c) {
        consumers.emplace_back([&queue, &consumed, expected, deadline]() {
            while (consumed.load(std::memory_order_relaxed) < expected && std::chrono::steady_clock::now() < deadline) {
                if (auto task = queue.try_pop()) {
                    (*task)();
                    consumed.fetch_add(1, std::memory_order_relaxed);
                } else {
                    std::this_thread::yield();
                }
            }
        });
    }
    return consumers;
}

template <typename Pred>
bool waitFor(Pred &&pred, std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return true;
}

}  // namespace test_helpers
