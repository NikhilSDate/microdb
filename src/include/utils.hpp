#include <condition_variable>
#include <mutex>
#include <queue>

// unbounded channel
template <typename T>
class Channel {
        void send(T val);
        T receive();
    private:
        std::queue<T> queue_;
        std::mutex m_;
        std::condition_variable nonempty_; 
};

template<typename T>
void Channel<T>::send(T val) {
    std::lock_guard<std::mutex> g(m_);
    queue_.push(std::move(val));
    nonempty_.notify_all();
}

template<typename T>
T Channel<T>::receive() {
    std::unique_lock<std::mutex> g(m_);
    nonempty_.wait(g, [&]() {return !queue_.empty(); });
    T data = std::move(queue_.front());
    queue_.pop();
    return std::move(data);
}