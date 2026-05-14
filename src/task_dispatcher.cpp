#include "task_dispatcher.hpp"

namespace dispatcher {
TaskDispatcher::TaskDispatcher(size_t thread_count, const queue::PriorityQueue::Config &config)
    : thread_pool_(thread_pool::ThreadPool{std::make_shared<queue::PriorityQueue>(config), thread_count}) {}

void TaskDispatcher::schedule(TaskPriority priority, queue::Task task) { thread_pool_.push(priority, std::move(task)); }

TaskDispatcher::~TaskDispatcher() = default;

}  // namespace dispatcher