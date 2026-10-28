#pragma once
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

// Minimal HTTP/1.1: Content-Length bodies only. Transfer-Encoding is rejected
// (unsupported, and ambiguous framing is how request smuggling works).

struct Header {
  std::string name, value;
};

struct HttpRequest {
  std::string method, target, version;
  std::vector<Header> headers;
  std::string body;
  bool keep_alive = true;
  const std::string* header(std::string_view name) const;  // case-insensitive, first match
};

struct HttpResponse {
  int status = 200;
  std::vector<Header> headers;  // serialize() adds Content-Length and Connection itself
  std::string body;
  bool keep_alive = true;
  const std::string* header(std::string_view name) const;
};

enum class ParseStatus { Incomplete, Done, Error, TooLarge };

constexpr size_t kMaxHeaderBytes = 16 * 1024;
constexpr size_t kMaxBodyBytes = 1 << 20;

// On Done the message's bytes are erased from the front of buf; pipelined bytes stay.
ParseStatus parse_request(std::string& buf, HttpRequest& out);
ParseStatus parse_response(std::string& buf, HttpResponse& out);

std::string serialize(const HttpRequest& req);    // drops hop-by-hop headers, sets Content-Length
std::string serialize(const HttpResponse& resp);  // sets Content-Length, Connection: close if needed
const char* reason_phrase(int status);
