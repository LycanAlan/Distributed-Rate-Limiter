#include "http.h"

#include <cctype>
#include <cstring>

namespace {

bool iequals(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); i++)
    if (std::tolower((unsigned char)a[i]) != std::tolower((unsigned char)b[i])) return false;
  return true;
}

std::string_view trim(std::string_view s) {
  while (!s.empty() && (s.front() == ' ' || s.front() == '\t')) s.remove_prefix(1);
  while (!s.empty() && (s.back() == ' ' || s.back() == '\t')) s.remove_suffix(1);
  return s;
}

// RFC 9110 token characters (header names, methods).
bool is_tchar(char c) {
  return std::isalnum((unsigned char)c) || (c && std::strchr("!#$%&'*+-.^_`|~", c));
}

bool is_token(std::string_view s) {
  if (s.empty()) return false;
  for (char c : s)
    if (!is_tchar(c)) return false;
  return true;
}

const std::string* find_header(const std::vector<Header>& hs, std::string_view name) {
  for (const auto& h : hs)
    if (iequals(h.name, name)) return &h.value;
  return nullptr;
}

// "keep-alive, Upgrade" contains token "keep-alive"
bool has_token(std::string_view list, std::string_view token) {
  while (!list.empty()) {
    size_t comma = list.find(',');
    if (iequals(trim(list.substr(0, comma)), token)) return true;
    if (comma == std::string_view::npos) break;
    list.remove_prefix(comma + 1);
  }
  return false;
}

bool keep_alive_for(std::string_view version, const std::vector<Header>& hs) {
  const std::string* conn = find_header(hs, "Connection");
  if (version == "HTTP/1.1") return !(conn && has_token(*conn, "close"));
  return conn && has_token(*conn, "keep-alive");
}

struct Framed {
  std::string_view start, headers;
  size_t header_end = 0;  // offset of the first body byte
};

// Finds the end of the header block. Doesn't look at the body.
ParseStatus frame(const std::string& buf, Framed& f) {
  size_t end = buf.find("\r\n\r\n");
  if (end == std::string::npos) {
    bool too_long_to_ever_finish = buf.size() > kMaxHeaderBytes;
    return too_long_to_ever_finish ? ParseStatus::TooLarge : ParseStatus::Incomplete;
  }
  if (end > kMaxHeaderBytes) return ParseStatus::TooLarge;
  std::string_view head(buf.data(), end);
  size_t eol = head.find("\r\n");
  f.start = head.substr(0, eol);
  f.headers = eol == std::string_view::npos ? std::string_view() : head.substr(eol + 2);
  f.header_end = end + 4;
  return ParseStatus::Done;
}

bool parse_headers(std::string_view block, std::vector<Header>& out) {
  out.clear();
  while (!block.empty()) {
    size_t eol = block.find("\r\n");
    std::string_view line = block.substr(0, eol);
    block.remove_prefix(eol == std::string_view::npos ? block.size() : eol + 2);
    size_t colon = line.find(':');
    if (colon == std::string_view::npos) return false;
    std::string_view name = line.substr(0, colon);
    if (!is_token(name)) return false;  // also rejects obsolete line folding (leading space)
    out.push_back({std::string(name), std::string(trim(line.substr(colon + 1)))});
  }
  return true;
}

ParseStatus content_length(const std::vector<Header>& hs, bool required, size_t& len) {
  constexpr size_t kMaxDigits = 15;  // far above kMaxBodyBytes, and can't overflow size_t
  if (find_header(hs, "Transfer-Encoding")) return ParseStatus::Error;
  bool seen = false;
  len = 0;
  for (const auto& h : hs) {
    if (!iequals(h.name, "Content-Length")) continue;
    if (h.value.empty() || h.value.size() > kMaxDigits) return ParseStatus::Error;
    size_t v = 0;
    for (char c : h.value) {
      if (!std::isdigit((unsigned char)c)) return ParseStatus::Error;
      v = v * 10 + (c - '0');
    }
    if (seen && v != len) return ParseStatus::Error;
    seen = true;
    len = v;
  }
  if (required && !seen) return ParseStatus::Error;  // read-until-close bodies: not supported
  return len > kMaxBodyBytes ? ParseStatus::TooLarge : ParseStatus::Done;
}

// ponytail: headers are re-parsed on every call until the body arrives; fine for
// small bodies, keep parse state across calls if large uploads ever matter.
template <class Msg>
ParseStatus take_body(std::string& buf, const Framed& f, size_t len, Msg& out) {
  if (buf.size() < f.header_end + len) return ParseStatus::Incomplete;
  out.body = buf.substr(f.header_end, len);
  buf.erase(0, f.header_end + len);
  return ParseStatus::Done;
}

bool is_hop_by_hop(std::string_view name) {
  for (const char* h : {"Connection", "Keep-Alive", "Proxy-Connection", "TE", "Trailer",
                        "Transfer-Encoding", "Upgrade", "Content-Length"})
    if (iequals(name, h)) return true;
  return false;
}

bool status_has_no_body(int status) { return status < 200 || status == 204 || status == 304; }

bool is_supported_version(std::string_view v) { return v == "HTTP/1.1" || v == "HTTP/1.0"; }

struct RequestLine {
  std::string_view method, target, version;
};

// METHOD SP target SP version, exactly one space between parts.
bool parse_request_line(std::string_view line, RequestLine& out) {
  size_t first_space = line.find(' ');
  if (first_space == std::string_view::npos) return false;
  size_t second_space = line.find(' ', first_space + 1);
  if (second_space == std::string_view::npos) return false;

  out.method = line.substr(0, first_space);
  out.target = line.substr(first_space + 1, second_space - first_space - 1);
  out.version = line.substr(second_space + 1);
  return is_token(out.method) && !out.target.empty() && is_supported_version(out.version);
}

struct StatusLine {
  std::string_view version;
  int status = 0;
};

// version SP 3DIGIT [SP reason-phrase]
bool parse_status_line(std::string_view line, StatusLine& out) {
  constexpr size_t kVersionLen = 8;                 // "HTTP/1.1"
  constexpr size_t kStatusStart = kVersionLen + 1;  // after the space
  constexpr size_t kStatusLen = 3;
  if (line.size() < kStatusStart + kStatusLen || line[kVersionLen] != ' ') return false;

  out.version = line.substr(0, kVersionLen);
  if (!is_supported_version(out.version)) return false;

  std::string_view digits = line.substr(kStatusStart, kStatusLen);
  out.status = 0;
  for (char c : digits) {
    if (!std::isdigit((unsigned char)c)) return false;
    out.status = out.status * 10 + (c - '0');
  }
  bool has_reason = line.size() > kStatusStart + kStatusLen;
  return !has_reason || line[kStatusStart + kStatusLen] == ' ';
}

}  // namespace

const std::string* HttpRequest::header(std::string_view name) const { return find_header(headers, name); }
const std::string* HttpResponse::header(std::string_view name) const { return find_header(headers, name); }

ParseStatus parse_request(std::string& buf, HttpRequest& out) {
  Framed f;
  ParseStatus s = frame(buf, f);
  if (s != ParseStatus::Done) return s;

  RequestLine line;
  if (!parse_request_line(f.start, line)) return ParseStatus::Error;
  if (!parse_headers(f.headers, out.headers)) return ParseStatus::Error;

  size_t body_len = 0;
  s = content_length(out.headers, /*required=*/false, body_len);
  if (s != ParseStatus::Done) return s;

  out.method = line.method;
  out.target = line.target;
  out.version = line.version;
  out.keep_alive = keep_alive_for(line.version, out.headers);
  return take_body(buf, f, body_len, out);
}

ParseStatus parse_response(std::string& buf, HttpResponse& out) {
  Framed f;
  ParseStatus s = frame(buf, f);
  if (s != ParseStatus::Done) return s;

  StatusLine line;
  if (!parse_status_line(f.start, line)) return ParseStatus::Error;
  if (!parse_headers(f.headers, out.headers)) return ParseStatus::Error;

  size_t body_len = 0;
  if (!status_has_no_body(line.status)) {
    s = content_length(out.headers, /*required=*/true, body_len);
    if (s != ParseStatus::Done) return s;
  }

  out.status = line.status;
  out.keep_alive = keep_alive_for(line.version, out.headers);
  return take_body(buf, f, body_len, out);
}

std::string serialize(const HttpRequest& r) {
  std::string s = r.method + " " + r.target + " HTTP/1.1\r\n";
  for (const auto& h : r.headers)
    if (!is_hop_by_hop(h.name)) s += h.name + ": " + h.value + "\r\n";
  if (!r.body.empty() || r.method == "POST" || r.method == "PUT" || r.method == "PATCH")
    s += "Content-Length: " + std::to_string(r.body.size()) + "\r\n";
  s += "\r\n";
  s += r.body;
  return s;
}

std::string serialize(const HttpResponse& r) {
  std::string s = "HTTP/1.1 " + std::to_string(r.status) + " " + reason_phrase(r.status) + "\r\n";
  for (const auto& h : r.headers)
    if (!is_hop_by_hop(h.name)) s += h.name + ": " + h.value + "\r\n";
  if (!status_has_no_body(r.status)) s += "Content-Length: " + std::to_string(r.body.size()) + "\r\n";
  if (!r.keep_alive) s += "Connection: close\r\n";
  s += "\r\n";
  if (!status_has_no_body(r.status)) s += r.body;
  return s;
}

const char* reason_phrase(int status) {
  switch (status) {
    case 200: return "OK";
    case 204: return "No Content";
    case 400: return "Bad Request";
    case 404: return "Not Found";
    case 413: return "Payload Too Large";
    case 429: return "Too Many Requests";
    case 431: return "Request Header Fields Too Large";
    case 500: return "Internal Server Error";
    case 501: return "Not Implemented";
    case 502: return "Bad Gateway";
    case 503: return "Service Unavailable";
    case 504: return "Gateway Timeout";
    default: return "Unknown";
  }
}
