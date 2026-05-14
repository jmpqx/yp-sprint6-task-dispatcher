#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "queue/priority_queue.hpp"
#include "test_helpers.hpp"
#include "types.hpp"

using namespace dispatcher::queue;
using namespace dispatcher;
using namespace test_helpers;
using namespace std::chrono_literals;

static PriorityQueue::Config defaultConfig() {
    return {{TaskPriority::High, {true, 64}}, {TaskPriority::Normal, {false}}};
}

TEST(PriorityQueue, PushHighThenPop) {
    PriorityQueue q(defaultConfig());
    std::atomic<int> counter{0};
    q.push(TaskPriority::High, makeCountingTask(counter));
    auto task = q.pop();

    ASSERT_TRUE(task.has_value());
    (*task)();
    EXPECT_EQ(counter.load(), 1);

    q.shutdown();
}

TEST(PriorityQueue, PushNormalThenPop) {
    PriorityQueue q(defaultConfig());
    std::atomic<int> counter{0};
    q.push(TaskPriority::Normal, makeCountingTask(counter));
    auto task = q.pop();

    ASSERT_TRUE(task.has_value());
    (*task)();
    EXPECT_EQ(counter.load(), 1);

    q.shutdown();
}

TEST(PriorityQueue, HighPriorityTakenBeforeNormal) {
    PriorityQueue q(defaultConfig());
    std::vector<int> order;
    std::mutex mtx;

    for (int i = 0; i < 3; ++i) {
        q.push(TaskPriority::Normal, [&order, &mtx, i]() {
            std::lock_guard lk(mtx);
            order.push_back(100 + i);
        });
    }

    for (int i = 0; i < 3; ++i) {
        q.push(TaskPriority::High, [&order, &mtx, i]() {
            std::lock_guard lk(mtx);
            order.push_back(i);
        });
    }

    for (int i = 0; i < 6; ++i) {
        auto t = q.pop();
        ASSERT_TRUE(t.has_value());
        (*t)();
    }

    for (int i = 0; i < 3; ++i)
        EXPECT_LT(order[i], 100);
    for (int i = 3; i < 6; ++i)
        EXPECT_GE(order[i], 100);

    q.shutdown();
}

TEST(PriorityQueue, PopBlocksUntilTaskAvailable) {
    PriorityQueue q(defaultConfig());
    std::atomic<int> counter{0};
    std::atomic<bool> popped{false};

    std::thread consumer([&]() {
        auto task = q.pop();
        if (task)
            (*task)();
        popped.store(true);
    });

    std::this_thread::sleep_for(30ms);
    EXPECT_FALSE(popped.load());

    q.push(TaskPriority::Normal, makeCountingTask(counter));
    EXPECT_TRUE(waitFor([&]() { return popped.load(); }, 2s));
    consumer.join();

    EXPECT_EQ(counter.load(), 1);
    q.shutdown();
}

TEST(PriorityQueue, ShutdownUnblocksBlockedPop) {
    PriorityQueue q(defaultConfig());
    std::atomic<bool> finished{false};

    std::thread consumer([&]() {
        q.pop();
        finished.store(true);
    });

    std::this_thread::sleep_for(30ms);
    EXPECT_FALSE(finished.load());

    q.shutdown();
    EXPECT_TRUE(waitFor([&]() { return finished.load(); }, 2s));
    consumer.join();
}

TEST(PriorityQueue, PopAfterShutdownOnEmptyReturnsNullopt) {
    PriorityQueue q(defaultConfig());
    q.shutdown();

    auto task = q.pop();
    EXPECT_FALSE(task.has_value());
}

TEST(PriorityQueue, PushAfterShutdownDropsTask) {
    PriorityQueue q(defaultConfig());
    q.shutdown();
    std::atomic<int> counter{0};

    q.push(TaskPriority::High, makeCountingTask(counter));
    q.push(TaskPriority::Normal, makeCountingTask(counter));
    EXPECT_EQ(counter.load(), 0);
}

TEST(PriorityQueue, EmptyConfigThrows) {
    PriorityQueue::Config empty_cfg;
    EXPECT_THROW(PriorityQueue{empty_cfg}, std::invalid_argument);
}

TEST(PriorityQueue, BoundedWithoutCapacityThrows) {
    PriorityQueue::Config cfg{{TaskPriority::High, {true, std::nullopt}}};
    EXPECT_THROW(PriorityQueue{cfg}, std::invalid_argument);
}

TEST(PriorityQueue, ManyProducersFewConsumers) {
    const int PRODUCERS = 8;
    const int TASKS_PER_PRODUCER = 100;
    const int TOTAL = PRODUCERS * TASKS_PER_PRODUCER;
    const int CONSUMERS = 2;

    PriorityQueue q(defaultConfig());
    std::atomic<int> done{0};
    std::atomic<int> consumed{0};

    std::vector<std::thread> consumers;
    for (int c = 0; c < CONSUMERS; ++c) {
        consumers.emplace_back([&]() {
            while (consumed.load() < TOTAL) {
                auto task = q.pop();
                if (task) {
                    (*task)();
                    consumed.fetch_add(1);
                } else {
                    break;
                }
            }
        });
    }

    std::vector<std::thread> producers;
    for (int p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < TASKS_PER_PRODUCER; ++i) {
                auto priority = (i % 2 == 0) ? TaskPriority::High : TaskPriority::Normal;
                q.push(priority, makeCountingTask(done));
            }
        });
    }

    for (auto &t : producers)
        t.join();
    EXPECT_TRUE(waitFor([&]() { return consumed.load() >= TOTAL; }, 15s));

    q.shutdown();
    for (auto &t : consumers)
        t.join();

    EXPECT_EQ(done.load(), TOTAL);
}

TEST(PriorityQueue, StressMixedPriorities) {
    const int THREADS = 4;
    const int TASKS_PER_THREAD = 500;
    const int TOTAL = THREADS * TASKS_PER_THREAD;

    PriorityQueue q(defaultConfig());
    std::atomic<int> done{0};
    std::atomic<int> consumed{0};

    std::vector<std::thread> consumers;
    for (int c = 0; c < THREADS; ++c) {
        consumers.emplace_back([&]() {
            while (true) {
                auto task = q.pop();
                if (!task)
                    break;
                (*task)();
                consumed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::vector<std::thread> producers;
    for (int p = 0; p < THREADS; ++p) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < TASKS_PER_THREAD; ++i) {
                auto priority = (p % 2 == 0) ? TaskPriority::High : TaskPriority::Normal;
                q.push(priority, makeCountingTask(done));
            }
        });
    }

    for (auto &t : producers)
        t.join();
    EXPECT_TRUE(waitFor([&]() { return consumed.load() >= TOTAL; }, 15s));

    q.shutdown();
    for (auto &t : consumers)
        t.join();

    EXPECT_EQ(done.load(), TOTAL);
}
