#pragma once
#include "queue/bounded_queue.hpp"
#include "queue/queue.hpp"
#include "queue/unbounded_queue.hpp"
#include "types.hpp"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <unordered_map>

namespace dispatcher::queue {

class PriorityQueue {
    using Queues = std::unordered_map<TaskPriority, std::unique_ptr<IQueue>>;

    Queues queues_;
    std::mutex mutex_;
    std::condition_variable is_not_empty_;
    bool stop_{false};

public:
    using Config = std::unordered_map<TaskPriority, QueueOptions>;

    explicit PriorityQueue(const Config &config);

    void push(TaskPriority priority, Task task);
    // block on pop until shutdown is called
    // after that return std::nullopt on empty queue
    std::optional<Task> pop();

    void shutdown();

    ~PriorityQueue();
};

}  // namespace dispatcher::queue