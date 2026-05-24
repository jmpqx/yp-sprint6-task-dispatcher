#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "queue/unbounded_queue.hpp"
#include "test_helpers.hpp"

using namespace dispatcher::queue;
using namespace test_helpers;
using namespace std::chrono_literals;

TEST(UnboundedQueue, InitiallyEmpty) {
    UnboundedQueue q;
    EXPECT_TRUE(q.empty());
}

TEST(UnboundedQueue, PushThenTryPop) {
    UnboundedQueue q;
    std::atomic<int> counter{0};
    q.push(makeCountingTask(counter));

    EXPECT_FALSE(q.empty());
    auto task = q.try_pop();
    ASSERT_TRUE(task.has_value());

    (*task)();

    EXPECT_EQ(counter.load(), 1);
    EXPECT_TRUE(q.empty());
}

TEST(UnboundedQueue, TryPopOnEmpty) {
    UnboundedQueue q;
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(UnboundedQueue, FIFOOrder) {
    UnboundedQueue q;
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

TEST(UnboundedQueue, AcceptsLargeNumberOfTasks) {
    UnboundedQueue q;
    const int N = 10000;
    std::atomic<int> counter{0};
    for (int i = 0; i < N; ++i) {
        q.push(makeCountingTask(counter));
    }

    EXPECT_FALSE(q.empty());

    int popped = 0;
    while (auto t = q.try_pop()) {
        (*t)();
        ++popped;
    }

    EXPECT_EQ(popped, N);
    EXPECT_EQ(counter.load(), N);
    EXPECT_TRUE(q.empty());
}

TEST(UnboundedQueue, PushAfterShutdownDropsTask) {
    UnboundedQueue q;
    q.shutdown();
    std::atomic<int> counter{0};
    q.push(makeCountingTask(counter));
    EXPECT_TRUE(q.empty());
    EXPECT_EQ(counter.load(), 0);
}

TEST(UnboundedQueue, TryPopAfterShutdownReturnsResidualTasks) {
    UnboundedQueue q;
    std::atomic<int> counter{0};
    q.push(makeCountingTask(counter));
    q.push(makeCountingTask(counter));
    q.shutdown();

    ASSERT_TRUE(q.try_pop().has_value());
    ASSERT_TRUE(q.try_pop().has_value());
    EXPECT_FALSE(q.try_pop().has_value());
}

TEST(UnboundedQueue, ManyProducersFewConsumers) {
    const int PRODUCERS = 8;
    const int TASKS_PER_PRODUCER = 200;
    const int TOTAL = PRODUCERS * TASKS_PER_PRODUCER;
    const int CONSUMERS = 2;

    UnboundedQueue q;
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

TEST(UnboundedQueue, FewProducersManyConsumers) {
    const int PRODUCERS = 2;
    const int TASKS_PER_PRODUCER = 500;
    const int TOTAL = PRODUCERS * TASKS_PER_PRODUCER;
    const int CONSUMERS = 8;

    UnboundedQueue q;
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

TEST(UnboundedQueue, StressEqualProducersConsumers) {
    const int THREADS = 8;
    const int TASKS_PER_THREAD = 1000;
    const int TOTAL = THREADS * TASKS_PER_THREAD;

    UnboundedQueue q;
    std::atomic<int> done{0};
    std::atomic<int> consumed{0};

    auto consumers = spawnConsumers(q, THREADS, consumed, TOTAL);
    auto producers = spawnProducers(q, THREADS, TASKS_PER_THREAD, [&done](int) { return makeCountingTask(done); });

    for (auto &t : producers)
        t.join();
    EXPECT_TRUE(waitFor([&]() { return consumed.load() >= TOTAL; }, 20s));
    for (auto &t : consumers)
        t.join();

    EXPECT_EQ(done.load(), TOTAL);
}

TEST(UnboundedQueue, ConcurrentPushAfterShutdownNoHang) {
    UnboundedQueue q;
    q.shutdown();

    std::vector<std::thread> threads;

    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([&q]() { q.push([]() {}); });
    }

    for (auto &t : threads)
        t.join();

    EXPECT_TRUE(q.empty());
}
