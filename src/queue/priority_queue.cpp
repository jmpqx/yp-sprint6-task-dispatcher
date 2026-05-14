#include "queue/priority_queue.hpp"
#include "queue/queue.hpp"
#include "types.hpp"
#include <mutex>

namespace dispatcher::queue {
PriorityQueue::PriorityQueue(const Config &config) {
    if (config.empty()) {
        throw std::invalid_argument("Config cannot be empty");
    }

    std::ranges::for_each(config, [this](auto &&pair) {
        const auto &[priority, options] = pair;

        if (options.bounded) {
            if (!options.capacity) {
                throw std::invalid_argument("Bounded queue needs specified capacity");
            }

            queues_.try_emplace(priority, std::make_unique<BoundedQueue>(options.capacity.value()));
        } else {
            queues_.try_emplace(priority, std::make_unique<UnboundedQueue>());
        }
    });
}

void PriorityQueue::push(TaskPriority priority, Task task) {
    {
        std::lock_guard lk(mutex_);
        if (stop_) {
            return;
        }
    }

    queues_.at(priority)->push(std::move(task));
    is_not_empty_.notify_one();
}

// block on pop until shutdown is called
// after that return std::nullopt on empty queue
std::optional<Task> PriorityQueue::pop() {
    std::unique_lock lk(mutex_);

    is_not_empty_.wait(lk, [&]() {
        return stop_ || std::ranges::any_of(queues_, [](auto &&pair) {
                   const auto &[priority, queue] = pair;

                   return !queue->empty();
               });
    });

    auto pop_with_retry = [](auto *queue) {
        std::expected<Task, Op> result;

        do {
            result = queue->try_pop();
            std::this_thread::yield();
        } while (!result && result.error() == Op::Locked);

        return result;
    };

    return pop_with_retry(queues_.at(TaskPriority::High).get())
        .or_else([&](auto op) { return pop_with_retry(queues_.at(TaskPriority::Normal).get()); })
        .transform([](auto &&task) -> std::optional<Task> { return std::move(task); })
        .value_or(std::nullopt);
}

void PriorityQueue::shutdown() {
    {
        std::lock_guard lk(mutex_);
        stop_ = true;

        std::ranges::for_each(queues_, [](auto &&pair) {
            const auto &[priority, queue] = pair;
            queue->shutdown();
        });
    }

    is_not_empty_.notify_all();
}

PriorityQueue::~PriorityQueue() = default;

}  // namespace dispatcher::queue