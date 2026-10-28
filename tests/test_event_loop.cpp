#include <gtest/gtest.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <vector>

#include "event_loop.h"

TEST(EventLoop, TimersFireInDeadlineOrder) {
  EventLoop loop;
  std::vector<int> order;
  loop.add_timer(30, [&] { order.push_back(30); loop.stop(); });
  loop.add_timer(10, [&] { order.push_back(10); });
  loop.add_timer(20, [&] { order.push_back(20); });
  loop.run();
  EXPECT_EQ(order, (std::vector<int>{10, 20, 30}));
}

TEST(EventLoop, CancelledTimerNeverFires) {
  EventLoop loop;
  bool fired = false;
  uint64_t id = loop.add_timer(5, [&] { fired = true; });
  loop.cancel_timer(id);
  loop.add_timer(20, [&] { loop.stop(); });
  loop.run();
  EXPECT_FALSE(fired);
}

TEST(EventLoop, TimerCanScheduleAnotherTimer) {
  EventLoop loop;
  int n = 0;
  loop.add_timer(1, [&] {
    n++;
    loop.add_timer(1, [&] { n++; loop.stop(); });
  });
  loop.run();
  EXPECT_EQ(n, 2);
}

TEST(EventLoop, ReadableFdFiresCallback) {
  EventLoop loop;
  int p[2];
  ASSERT_EQ(pipe(p), 0);
  uint32_t got = 0;
  loop.add_fd(p[0], EPOLLIN, [&](uint32_t ev) {
    got = ev;
    char b;
    ASSERT_EQ(read(p[0], &b, 1), 1);
    loop.stop();
  });
  loop.add_timer(1000, [&] { loop.stop(); });  // safety net
  ASSERT_EQ(write(p[1], "x", 1), 1);
  loop.run();
  EXPECT_TRUE(got & EPOLLIN);
  close(p[0]);
  close(p[1]);
}

TEST(EventLoop, DeletedFdGetsNoStaleEventFromTheSameBatch) {
  // Both pipes are readable, so one epoll_wait returns both events. Whichever
  // callback runs first deletes both fds; the other callback must not run.
  EventLoop loop;
  int a[2], b[2];
  ASSERT_EQ(pipe(a), 0);
  ASSERT_EQ(pipe(b), 0);
  int calls = 0;
  auto cb = [&](uint32_t) {
    calls++;
    loop.del_fd(a[0]);  // deleting our own fd from inside our callback must be safe
    loop.del_fd(b[0]);
  };
  loop.add_fd(a[0], EPOLLIN, cb);
  loop.add_fd(b[0], EPOLLIN, cb);
  ASSERT_EQ(write(a[1], "x", 1), 1);
  ASSERT_EQ(write(b[1], "x", 1), 1);
  loop.add_timer(50, [&] { loop.stop(); });
  loop.run();
  EXPECT_EQ(calls, 1);
  for (int fd : {a[0], a[1], b[0], b[1]}) close(fd);
}

TEST(EventLoop, ModFdSwitchesInterest) {
  EventLoop loop;
  int p[2];
  ASSERT_EQ(pipe(p), 0);
  int writable = 0;
  loop.add_fd(p[1], 0, [&](uint32_t ev) {
    if (ev & EPOLLOUT) writable++;
    loop.mod_fd(p[1], 0);  // stop asking, or level-triggered EPOLLOUT spins forever
  });
  loop.add_timer(10, [&] { loop.mod_fd(p[1], EPOLLOUT); });
  loop.add_timer(60, [&] { loop.stop(); });
  loop.run();
  EXPECT_EQ(writable, 1);
  close(p[0]);
  close(p[1]);
}
