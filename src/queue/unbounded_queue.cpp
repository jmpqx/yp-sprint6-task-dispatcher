#include "queue/unbounded_queue.hpp"

#include <functional>
#include <mutex>
#include <queue>
#include <semaphore>

namespace dispatcher::queue {
UnboundedQueue::UnboundedQueue() = default;

void UnboundedQueue::push(Task task) {
    std::lock_guard lk(mutex_);

    if (stop_) {
        return;
    }

    queue_.push(std::move(task));
}

std::expected<Task, Op> UnboundedQueue::try_pop() {
    if (mutex_.try_lock()) {
        if (queue_.empty()) {
            mutex_.unlock();
            return std::unexpected(Op::Empty);
        }

        auto task = std::move(queue_.front());
        queue_.pop();

        mutex_.unlock();
        return task;
    }

    return std::unexpected(Op::Locked);
}

bool UnboundedQueue::empty() const {
    std::lock_guard lk(mutex_);
    return queue_.empty();
}

void UnboundedQueue::shutdown() {
    std::lock_guard lk(mutex_);
    stop_ = true;
}

UnboundedQueue::~UnboundedQueue() = default;

}  // namespace dispatcher::queue