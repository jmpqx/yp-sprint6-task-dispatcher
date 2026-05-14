#pragma once

#include <thread>
#include <vector>

#include "queue/priority_queue.hpp"
#include "queue/queue.hpp"
#include "types.hpp"

namespace dispatcher::thread_pool {

class ThreadPool {
    std::vector<std::jthread> threads_;
    std::shared_ptr<queue::PriorityQueue> task_queue_;
    std::atomic<bool> stop_{false};

    void workerRoutine_();

public:
    explicit ThreadPool(std::shared_ptr<queue::PriorityQueue> task_queue, size_t thread_count);
    ~ThreadPool();

    void push(TaskPriority priority, queue::Task task);
};

}  // namespace dispatcher::thread_pool
