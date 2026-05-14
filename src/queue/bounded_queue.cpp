#include "queue/bounded_queue.hpp"
#include <mutex>
#include <optional>

namespace dispatcher::queue {
BoundedQueue::BoundedQueue(int capacity) : capacity_(capacity) {}

void BoundedQueue::push(Task task) {
    std::unique_lock lk(mutex_);

    is_not_full_.wait(lk, [&]() { return (queue_.size() != capacity_) || stop_; });

    if (stop_) {
        return;
    }

    queue_.push(std::move(task));
}

std::expected<Task, Op> BoundedQueue::try_pop() {
    Task task;

    {
        std::unique_lock lk(mutex_, std::defer_lock);
        if (!lk.try_lock()) {
            return std::unexpected(Op::Locked);
        }

        if (queue_.empty()) {
            return std::unexpected(Op::Empty);
        }

        task = std::move(queue_.front());
        queue_.pop();
    }

    is_not_full_.notify_one();

    return task;
}

bool BoundedQueue::empty() const {
    std::lock_guard lk(mutex_);
    return queue_.empty();
}

void BoundedQueue::shutdown() {
    {
        std::lock_guard lk(mutex_);
        stop_ = true;
    }

    is_not_full_.notify_all();
}

BoundedQueue::~BoundedQueue() = default;

}  // namespace dispatcher::queue