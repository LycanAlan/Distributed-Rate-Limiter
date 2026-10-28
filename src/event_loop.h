#pragma once
#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>
#include <vector>

// Single-threaded reactor: level-triggered epoll for fds, a min-heap for timers.
// Not thread-safe; everything runs on the thread that calls run().
class EventLoop {
 public:
  using IoCb = std::function<void(uint32_t events)>;  // EPOLLIN/EPOLLOUT/EPOLLERR/EPOLLHUP bits
  using TimerCb = std::function<void()>;

  EventLoop();
  ~EventLoop();
  EventLoop(const EventLoop&) = delete;
  EventLoop& operator=(const EventLoop&) = delete;

  void add_fd(int fd, uint32_t events, IoCb cb);
  void mod_fd(int fd, uint32_t events);
  void del_fd(int fd);  // safe inside any callback, including the fd's own
  uint64_t add_timer(int64_t delay_ms, TimerCb cb);
  void cancel_timer(uint64_t id);
  void run();   // until stop()
  void stop();  // run() returns after the current iteration
  static int64_t now_ms();  // steady_clock

 private:
  struct Timer {
    int64_t deadline;
    uint64_t id;  // tie-break: equal deadlines fire in insertion order
    bool operator>(const Timer& o) const {
      return deadline != o.deadline ? deadline > o.deadline : id > o.id;
    }
  };
  int timeout_ms();
  void run_due_timers();

  int epfd_;
  bool running_ = false;
  uint64_t next_id_ = 1;
  // epoll_event.data carries a registration id, not the fd: an event for an fd
  // that was deleted (and maybe reused) earlier in the same batch finds no handler.
  std::unordered_map<int, uint64_t> fd_ids_;
  std::unordered_map<uint64_t, std::shared_ptr<IoCb>> handlers_;
  std::priority_queue<Timer, std::vector<Timer>, std::greater<Timer>> timers_;
  std::unordered_map<uint64_t, TimerCb> timer_cbs_;  // cancel = erase; heap entry skipped later
};
