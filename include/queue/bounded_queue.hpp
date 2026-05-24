#pragma once
#include "queue/queue.hpp"
#include <condition_variable>
#include <mutex>
#include <queue>

namespace dispatcher::queue {

class BoundedQueue : public IQueue {
    std::queue<Task> queue_;
    mutable std::mutex mutex_;
    std::condition_variable is_not_full_;
    int capacity_;
    bool stop_{false};

public:
    explicit BoundedQueue(int capacity);

    void push(Task task) override;

    std::expected<Task, Op> try_pop() override;

    bool empty() const override;

    void shutdown() override;

    ~BoundedQueue() override;
};

}  // namespace dispatcher::queue