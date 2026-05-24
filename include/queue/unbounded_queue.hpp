#pragma once
#include "queue/queue.hpp"
#include <mutex>
#include <queue>

namespace dispatcher::queue {

class UnboundedQueue : public IQueue {
    std::queue<Task> queue_;
    mutable std::mutex mutex_;
    bool stop_{false};

public:
    explicit UnboundedQueue();

    void push(Task task) override;

    std::expected<Task, Op> try_pop() override;

    bool empty() const override;

    void shutdown() override;

    ~UnboundedQueue() override;
};

}  // namespace dispatcher::queue