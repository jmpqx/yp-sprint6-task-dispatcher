#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "queue/bounded_queue.hpp"
#include "test_helpers.hpp"

using namespace dispatcher::queue;
using namespace test_helpers;
using namespace std::chrono_literals;

TEST(BoundedQueue, InitiallyEmpty) {
    BoundedQueue q(4);
    EXPECT_TRUE(q.empty());
}

TEST(BoundedQueue, PushThenTryPop) {
    BoundedQueue q(4);
    std::atomic<int> counter{0};
    q.push(makeCountingTask(counter));
    EXPECT_FALSE(q.empty());
    auto task = q.try_pop();
    ASSERT_TRUE(task.has_value());
    (*task)();
    EXPECT_EQ(counter.load(), 1);
    EXPECT_TRUE(q.empty());
}

TEST(BoundedQueue, TryPopOnEmpty) {
    BoundedQueue q(4);
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(BoundedQueue, FIFOOrder) {
    BoundedQueue q(8);
    std::vector<int> order;
    std::mutex mtx;
    for (int i = 0; i < 5; ++i) {
        int val = i;
        q.push([val, &order, &mtx]() {
            std::lock_guard lk(mtx);
            order.push_back(val);
        });
    }
    for (int i = 0; i < 5; ++i) {
        auto t = q.try_pop();
        ASSERT_TRUE(t.has_value());
        (*t)();
    }
    EXPECT_EQ(order, (std::vector<int>{0, 1, 2, 3, 4}));
}

TEST(BoundedQueue, BlocksWhenFull) {
    BoundedQueue q(2);
    std::atomic<int> pushed{0};

    q.push(makeCountingTask(pushed));
    q.push(makeCountingTask(pushed));

    std::thread producer([&]() { q.push(makeCountingTask(pushed)); });

    std::this_thread::sleep_for(30ms);
    EXPECT_EQ(pushed.load(), 0);

    q.try_pop();
    EXPECT_TRUE(waitFor([&]() { return producer.joinable() && pushed.load() == 0; }, 2s));
    producer.join();
}

TEST(BoundedQueue, PushAfterShutdownDropsTask) {
    BoundedQueue q(4);
    q.shutdown();
    std::atomic<int> counter{0};
    q.push(makeCountingTask(counter));
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(counter.load(), 0);
}

TEST(BoundedQueue, ShutdownUnblocksBlockedPush) {
    BoundedQueue q(1);
    q.push([]() {});

    std::atomic<bool> finished{false};
    std::thread producer([&]() {
        q.push([]() {});
        finished.store(true);
    });

    std::this_thread::sleep_for(30ms);
    EXPECT_FALSE(finished.load());

    q.shutdown();
    EXPECT_TRUE(waitFor([&]() { return finished.load(); }, 2s));
    producer.join();
}

TEST(BoundedQueue, TryPopAfterShutdown) {
    BoundedQueue q(4);
    std::atomic<int> counter{0};
    q.push(makeCountingTask(counter));
    q.shutdown();

    auto task = q.try_pop();
    ASSERT_TRUE(task.has_value());
    (*task)();
    EXPECT_EQ(counter.load(), 1);
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(BoundedQueue, ManyProducersFewConsumers) {
    const int CAPACITY = 32;
    const int PRODUCERS = 8;
    const int TASKS_PER_PRODUCER = 50;
    const int TOTAL = PRODUCERS * TASKS_PER_PRODUCER;
    const int CONSUMERS = 2;

    BoundedQueue q(CAPACITY);
    std::atomic<int> done{0};
    std::atomic<int> consumed{0};

    auto consumers = spawnConsumers(q, CONSUMERS, consumed, TOTAL);

    auto producers = spawnProducers(q, PRODUCERS, TASKS_PER_PRODUCER, [&done](int) { return makeCountingTask(done); });

    for (auto &t : producers)
        t.join();
    EXPECT_TRUE(waitFor([&]() { return consumed.load() >= TOTAL; }, 10s));
    for (auto &t : consumers)
        t.join();

    EXPECT_EQ(done.load(), TOTAL);
}

TEST(BoundedQueue, FewProducersManyConsumers) {
    const int CAPACITY = 64;
    const int PRODUCERS = 2;
    const int TASKS_PER_PRODUCER = 100;
    const int TOTAL = PRODUCERS * TASKS_PER_PRODUCER;
    const int CONSUMERS = 8;

    BoundedQueue q(CAPACITY);
    std::atomic<int> done{0};
    std::atomic<int> consumed{0};

    auto consumers = spawnConsumers(q, CONSUMERS, consumed, TOTAL);
    auto producers = spawnProducers(q, PRODUCERS, TASKS_PER_PRODUCER, [&done](int) { return makeCountingTask(done); });

    for (auto &t : producers)
        t.join();
    EXPECT_TRUE(waitFor([&]() { return consumed.load() >= TOTAL; }, 10s));
    for (auto &t : consumers)
        t.join();

    EXPECT_EQ(done.load(), TOTAL);
}

TEST(BoundedQueue, StressEqualProducersConsumers) {
    const int CAPACITY = 16;
    const int THREADS = 4;
    const int TASKS_PER_THREAD = 500;
    const int TOTAL = THREADS * TASKS_PER_THREAD;

    BoundedQueue q(CAPACITY);
    std::atomic<int> done{0};
    std::atomic<int> consumed{0};

    auto consumers = spawnConsumers(q, THREADS, consumed, TOTAL);
    auto producers = spawnProducers(q, THREADS, TASKS_PER_THREAD, [&done](int) { return makeCountingTask(done); });

    for (auto &t : producers)
        t.join();
    EXPECT_TRUE(waitFor([&]() { return consumed.load() >= TOTAL; }, 15s));
    for (auto &t : consumers)
        t.join();

    EXPECT_EQ(done.load(), TOTAL);
}
