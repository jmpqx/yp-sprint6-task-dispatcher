#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <optional>

namespace dispatcher::queue {

using Task = std::function<void()>;

enum class Op : uint8_t { Locked, Empty };

struct QueueOptions {
    bool bounded;
    std::optional<int> capacity;
};

class IQueue {
public:
    virtual ~IQueue() = default;
    virtual void push(Task task) = 0;
    virtual std::expected<Task, Op> try_pop() = 0;
    virtual bool empty() const = 0;
    virtual void shutdown() = 0;
};

}  // namespace dispatcher::queue