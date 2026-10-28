#include "event_loop.h"

#include <sys/epoll.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstring>
#include <stdexcept>
#include <string>

static void check(int rc, const char* what) {
  if (rc < 0) throw std::runtime_error(std::string(what) + ": " + std::strerror(errno));
}

EventLoop::EventLoop() : epfd_(epoll_create1(EPOLL_CLOEXEC)) { check(epfd_, "epoll_create1"); }

EventLoop::~EventLoop() { close(epfd_); }

int64_t EventLoop::now_ms() {
  using namespace std::chrono;
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

void EventLoop::add_fd(int fd, uint32_t events, IoCb cb) {
  uint64_t id = next_id_++;
  epoll_event ev{};
  ev.events = events;
  ev.data.u64 = id;
  check(epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev), "epoll_ctl ADD");
  fd_ids_[fd] = id;
  handlers_[id] = std::make_shared<IoCb>(std::move(cb));
}

void EventLoop::mod_fd(int fd, uint32_t events) {
  auto it = fd_ids_.find(fd);
  if (it == fd_ids_.end()) return;
  epoll_event ev{};
  ev.events = events;
  ev.data.u64 = it->second;
  check(epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev), "epoll_ctl MOD");
}

void EventLoop::del_fd(int fd) {
  auto it = fd_ids_.find(fd);
  if (it == fd_ids_.end()) return;
  epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr);  // EBADF if already closed: the kernel dropped it anyway
  handlers_.erase(it->second);
  fd_ids_.erase(it);
}

uint64_t EventLoop::add_timer(int64_t delay_ms, TimerCb cb) {
  uint64_t id = next_id_++;
  timers_.push({now_ms() + delay_ms, id});
  timer_cbs_[id] = std::move(cb);
  return id;
}

void EventLoop::cancel_timer(uint64_t id) { timer_cbs_.erase(id); }

void EventLoop::stop() { running_ = false; }

int EventLoop::timeout_ms() {
  while (!timers_.empty() && !timer_cbs_.count(timers_.top().id)) timers_.pop();  // cancelled
  if (timers_.empty()) return -1;
  int64_t left = timers_.top().deadline - now_ms();
  return left <= 0 ? 0 : (left > INT_MAX ? INT_MAX : static_cast<int>(left));
}

void EventLoop::run_due_timers() {
  int64_t now = now_ms();
  while (!timers_.empty() && timers_.top().deadline <= now) {
    Timer t = timers_.top();
    timers_.pop();
    auto it = timer_cbs_.find(t.id);
    if (it == timer_cbs_.end()) continue;
    TimerCb cb = std::move(it->second);
    timer_cbs_.erase(it);
    cb();
  }
}

void EventLoop::run() {
  running_ = true;
  epoll_event events[128];
  while (running_) {
    int n = epoll_wait(epfd_, events, 128, timeout_ms());
    if (n < 0) {
      if (errno == EINTR) continue;
      check(n, "epoll_wait");
    }
    for (int i = 0; i < n; i++) {
      auto it = handlers_.find(events[i].data.u64);
      if (it == handlers_.end()) continue;  // deleted earlier in this batch
      std::shared_ptr<IoCb> cb = it->second;  // keeps the callback alive if it deletes itself
      (*cb)(events[i].events);
    }
    run_due_timers();
  }
}
