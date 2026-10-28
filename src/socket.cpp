#include "socket.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <stdexcept>

namespace {

constexpr int kListenBacklog = 1024;
constexpr size_t kReadChunk = 16 * 1024;

[[noreturn]] void throw_errno(const std::string& what) {
  throw std::runtime_error(what + ": " + std::strerror(errno));
}

void set_option(int fd, int level, int name) {
  int on = 1;
  if (setsockopt(fd, level, name, &on, sizeof on) < 0) throw_errno("setsockopt");
}

bool would_block(int err) { return err == EAGAIN || err == EWOULDBLOCK; }

}  // namespace

Fd& Fd::operator=(Fd&& other) noexcept {
  if (this != &other) {
    reset();
    fd_ = other.release();
  }
  return *this;
}

int Fd::release() {
  int fd = fd_;
  fd_ = -1;
  return fd;
}

void Fd::reset() {
  if (fd_ >= 0) close(fd_);
  fd_ = -1;
}

sockaddr_in resolve_ipv4(const std::string& host, int port) {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* result = nullptr;
  int rc = getaddrinfo(host.c_str(), nullptr, &hints, &result);
  if (rc != 0) throw std::runtime_error("resolve " + host + ": " + gai_strerror(rc));
  sockaddr_in addr = *reinterpret_cast<sockaddr_in*>(result->ai_addr);
  freeaddrinfo(result);
  addr.sin_port = htons(static_cast<uint16_t>(port));
  return addr;
}

Fd listen_tcp(int port) {
  Fd fd(socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  if (!fd) throw_errno("socket");
  set_option(fd.get(), SOL_SOCKET, SO_REUSEADDR);  // restart without waiting out TIME_WAIT
  set_option(fd.get(), SOL_SOCKET, SO_REUSEPORT);  // several processes may share the port

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(port));
  if (bind(fd.get(), reinterpret_cast<sockaddr*>(&addr), sizeof addr) < 0)
    throw_errno("bind :" + std::to_string(port));
  if (listen(fd.get(), kListenBacklog) < 0) throw_errno("listen");
  return fd;
}

Fd accept_client(int listen_fd) {
  return Fd(accept4(listen_fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC));
}

Fd connect_tcp(const sockaddr_in& addr) {
  Fd fd(socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
  if (!fd) return fd;
  int rc = connect(fd.get(), reinterpret_cast<const sockaddr*>(&addr), sizeof addr);
  if (rc < 0 && errno != EINPROGRESS) return Fd();
  set_nodelay(fd.get());
  return fd;
}

bool connect_succeeded(int fd) {
  int err = 0;
  socklen_t len = sizeof err;
  return getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0;
}

void set_nodelay(int fd) {
  int on = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);  // best effort; Nagle only adds latency here
}

std::string peer_ip(int fd) {
  sockaddr_in addr{};
  socklen_t len = sizeof addr;
  if (getpeername(fd, reinterpret_cast<sockaddr*>(&addr), &len) < 0) return "unknown";
  char text[INET_ADDRSTRLEN] = {};
  inet_ntop(AF_INET, &addr.sin_addr, text, sizeof text);
  return text;
}

IoResult read_available(int fd, std::string& into) {
  char chunk[kReadChunk];
  for (;;) {
    ssize_t n = read(fd, chunk, sizeof chunk);
    if (n > 0) {
      into.append(chunk, static_cast<size_t>(n));
      continue;
    }
    if (n == 0) return IoResult::Closed;
    if (errno == EINTR) continue;
    return would_block(errno) ? IoResult::Ok : IoResult::Error;
  }
}

IoResult write_available(int fd, std::string& from) {
  size_t sent = 0;
  while (sent < from.size()) {
    // MSG_NOSIGNAL: a peer that already closed gives EPIPE instead of killing us with SIGPIPE
    ssize_t n = send(fd, from.data() + sent, from.size() - sent, MSG_NOSIGNAL);
    if (n >= 0) {
      sent += static_cast<size_t>(n);
      continue;
    }
    if (errno == EINTR) continue;
    from.erase(0, sent);
    return would_block(errno) ? IoResult::WouldBlock : IoResult::Error;
  }
  from.clear();
  return IoResult::Ok;
}
