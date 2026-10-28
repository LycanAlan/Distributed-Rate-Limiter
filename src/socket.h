#pragma once
#include <netinet/in.h>

#include <string>

// Owns a file descriptor and closes it on destruction. Move-only.
class Fd {
 public:
  Fd() = default;
  explicit Fd(int fd) : fd_(fd) {}
  ~Fd() { reset(); }
  Fd(Fd&& other) noexcept : fd_(other.release()) {}
  Fd& operator=(Fd&& other) noexcept;
  Fd(const Fd&) = delete;
  Fd& operator=(const Fd&) = delete;

  int get() const { return fd_; }
  int release();
  void reset();
  explicit operator bool() const { return fd_ >= 0; }

 private:
  int fd_ = -1;
};

// Blocking name lookup: call at startup only, never on the event loop. Throws.
sockaddr_in resolve_ipv4(const std::string& host, int port);

// Non-blocking listener with SO_REUSEADDR and SO_REUSEPORT. Throws.
Fd listen_tcp(int port);

// Non-blocking accept. Returns an invalid Fd when nothing is pending.
Fd accept_client(int listen_fd);

// Starts a non-blocking connect. The socket turns writable when it finishes;
// then call connect_succeeded(). Returns an invalid Fd on immediate failure.
Fd connect_tcp(const sockaddr_in& addr);
bool connect_succeeded(int fd);

void set_nodelay(int fd);
std::string peer_ip(int fd);

enum class IoResult { Ok, WouldBlock, Closed, Error };

// Appends everything readable right now. Ok means the socket is drained.
IoResult read_available(int fd, std::string& into);

// Writes as much of `from` as the socket takes and erases it. Ok means all sent.
IoResult write_available(int fd, std::string& from);
