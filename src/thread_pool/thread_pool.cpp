#include "thread_pool/thread_pool.hpp"
#include <atomic>
#include <print>

namespace dispatcher::thread_pool {
void ThreadPool::workerRoutine_() {
    while (true) {
        if (auto task = task_queue_->pop()) {
            try {
                task.value()();
            } catch (const std::exception &e) {
                std::println("Exception in task: {}", e.what());
            }
        } else {
            if (stop_.load(std::memory_order_acquire)) {
                break;
            }
        }
    }
}

ThreadPool::ThreadPool(std::shared_ptr<queue::PriorityQueue> task_queue, size_t thread_count)
    : task_queue_(std::move(task_queue)), threads_(thread_count) {
    for (size_t i = 0; i < thread_count; i++) {
        threads_.emplace_back([&]() { workerRoutine_(); });
    }
}

ThreadPool::~ThreadPool() {
    task_queue_->shutdown();
    stop_.store(true, std::memory_order_release);

    for (auto &thread : threads_) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

void ThreadPool::push(TaskPriority priority, queue::Task task) {
    if (!stop_.load(std::memory_order_acquire)) {
        task_queue_->push(priority, std::move(task));
    }
}

}  // namespace dispatcher::thread_pool