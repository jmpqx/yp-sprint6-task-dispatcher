#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>

#include "task_dispatcher.hpp"
#include "test_helpers.hpp"

using namespace dispatcher;
using namespace test_helpers;
using namespace std::chrono_literals;

TEST(TaskDispatcher, ScheduleAndExecuteSingleTask) {
    TaskDispatcher dispatcher(2);
    std::atomic<int> counter{0};
    dispatcher.schedule(TaskPriority::Normal, makeCountingTask(counter));
    EXPECT_TRUE(waitFor([&]() { return counter.load() == 1; }, 5s));
}

TEST(TaskDispatcher, ScheduleHighAndNormalPriority) {
    TaskDispatcher dispatcher(2);
    std::atomic<int> counter{0};
    dispatcher.schedule(TaskPriority::High, makeCountingTask(counter));
    dispatcher.schedule(TaskPriority::Normal, makeCountingTask(counter));
    EXPECT_TRUE(waitFor([&]() { return counter.load() == 2; }, 5s));
}

TEST(TaskDispatcher, DestructorWaitsForAllTasks) {
    std::atomic<int> counter{0};

    {
        TaskDispatcher dispatcher(4);
        const int N = 200;
        for (int i = 0; i < N; ++i) {
            dispatcher.schedule(TaskPriority::Normal, makeCountingTask(counter));
        }
    }

    EXPECT_EQ(counter.load(), 200);
}

TEST(TaskDispatcher, HighPriorityExecutedBeforeNormal) {
    std::vector<int> order;
    std::mutex mtx;

    TaskDispatcher dispatcher(1);

    dispatcher.schedule(TaskPriority::Normal, makeLatencyTask(50ms));

    for (int i = 0; i < 3; ++i) {
        dispatcher.schedule(TaskPriority::Normal, [i, &order, &mtx]() {
            std::lock_guard lk(mtx);
            order.push_back(100 + i);
        });
    }

    for (int i = 0; i < 3; ++i) {
        dispatcher.schedule(TaskPriority::High, [i, &order, &mtx]() {
            std::lock_guard lk(mtx);
            order.push_back(i);
        });
    }

    EXPECT_TRUE(waitFor(
        [&]() {
            std::lock_guard lk(mtx);
            return order.size() == 6;
        },
        5s));

    {
        std::lock_guard lk(mtx);
        for (int i = 0; i < 3; ++i) {
            EXPECT_LT(order[i], 100) << "позиция " << i;
        }

        for (int i = 3; i < 6; ++i) {
            EXPECT_GE(order[i], 100) << "позиция " << i;
        }
    }
}

TEST(TaskDispatcher, AllTasksCompletedSingleThread) {
    const int N = 500;
    std::atomic<int> counter{0};
    TaskDispatcher dispatcher(1);

    for (int i = 0; i < N; ++i) {
        dispatcher.schedule(TaskPriority::Normal, makeCountingTask(counter));
    }

    EXPECT_TRUE(waitFor([&]() { return counter.load() == N; }, 10s));
}

TEST(TaskDispatcher, AllTasksCompletedManyThreads) {
    const int N = 2000;
    std::atomic<int> counter{0};
    TaskDispatcher dispatcher(8);

    for (int i = 0; i < N; ++i) {
        auto p = (i % 3 == 0) ? TaskPriority::High : TaskPriority::Normal;
        dispatcher.schedule(p, makeCountingTask(counter));
    }

    EXPECT_TRUE(waitFor([&]() { return counter.load() == N; }, 15s));
}

TEST(TaskDispatcher, ManyProducersFewWorkers) {
    const int PRODUCERS = 8;
    const int TASKS_PER_PRODUCER = 100;
    const int TOTAL = PRODUCERS * TASKS_PER_PRODUCER;

    std::atomic<int> counter{0};
    TaskDispatcher dispatcher(2);

    std::vector<std::thread> producers;

    for (int p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&]() {
            for (int i = 0; i < TASKS_PER_PRODUCER; ++i) {
                dispatcher.schedule(TaskPriority::Normal, makeCountingTask(counter));
            }
        });
    }

    for (auto &t : producers) {
        t.join();
    }

    EXPECT_TRUE(waitFor([&]() { return counter.load() == TOTAL; }, 15s));
}

TEST(TaskDispatcher, FewProducersManyWorkers) {
    const int PRODUCERS = 2;
    const int TASKS_PER_PRODUCER = 500;
    const int TOTAL = PRODUCERS * TASKS_PER_PRODUCER;

    std::atomic<int> counter{0};
    TaskDispatcher dispatcher(8);

    std::vector<std::thread> producers;
    for (int p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&]() {
            for (int i = 0; i < TASKS_PER_PRODUCER; ++i) {
                auto priority = (i % 2 == 0) ? TaskPriority::High : TaskPriority::Normal;
                dispatcher.schedule(priority, makeCountingTask(counter));
            }
        });
    }

    for (auto &t : producers) {
        t.join();
    }

    EXPECT_TRUE(waitFor([&]() { return counter.load() == TOTAL; }, 15s));
}

TEST(TaskDispatcher, StressTest) {
    const int PRODUCERS = 8;
    const int TASKS_PER_PRODUCER = 500;
    const int TOTAL = PRODUCERS * TASKS_PER_PRODUCER;

    std::atomic<int> counter{0};
    TaskDispatcher dispatcher(4);

    std::vector<std::thread> producers;
    for (int p = 0; p < PRODUCERS; ++p) {
        producers.emplace_back([&, p]() {
            for (int i = 0; i < TASKS_PER_PRODUCER; ++i) {
                auto priority = (p % 2 == 0) ? TaskPriority::High : TaskPriority::Normal;
                dispatcher.schedule(priority, makeCountingTask(counter));
            }
        });
    }

    for (auto &t : producers) {
        t.join();
    }

    EXPECT_TRUE(waitFor([&]() { return counter.load() == TOTAL; }, 20s));
}

TEST(TaskDispatcher, CustomConfig) {
    queue::PriorityQueue::Config cfg = {{TaskPriority::High, {true, 32}}, {TaskPriority::Normal, {true, 16}}};
    std::atomic<int> counter{0};
    TaskDispatcher dispatcher(4, cfg);
    const int N = 100;

    for (int i = 0; i < N; ++i) {
        dispatcher.schedule(TaskPriority::Normal, makeCountingTask(counter));
    }

    EXPECT_TRUE(waitFor([&]() { return counter.load() == N; }, 10s));
}

TEST(TaskDispatcher, ExceptionInTaskDoesNotKillWorker) {
    std::atomic<int> counter{0};
    TaskDispatcher dispatcher(2);

    for (int i = 0; i < 4; ++i) {
        dispatcher.schedule(TaskPriority::Normal, []() { throw std::runtime_error("test exception"); });
    }

    const int N = 50;

    for (int i = 0; i < N; ++i) {
        dispatcher.schedule(TaskPriority::Normal, makeCountingTask(counter));
    }

    EXPECT_TRUE(waitFor([&]() { return counter.load() == N; }, 10s));
}

TEST(TaskDispatcher, LargeVolumeHighPriorityTasks) {
    const int N = 1000;
    std::atomic<int> counter{0};

    queue::PriorityQueue::Config cfg = {{TaskPriority::High, {true, N}}, {TaskPriority::Normal, {false}}};
    TaskDispatcher dispatcher(4, cfg);

    for (int i = 0; i < N; ++i) {
        dispatcher.schedule(TaskPriority::High, makeCountingTask(counter));
    }

    EXPECT_TRUE(waitFor([&]() { return counter.load() == N; }, 15s));
}
