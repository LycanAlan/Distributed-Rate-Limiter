#include <gtest/gtest.h>

#include "http.h"

TEST(HttpRequest, ParsesSimpleGet) {
  std::string buf = "GET /api/x?y=1 HTTP/1.1\r\nHost: a\r\nX-User-Id: 42\r\n\r\n";
  HttpRequest r;
  ASSERT_EQ(parse_request(buf, r), ParseStatus::Done);
  EXPECT_EQ(r.method, "GET");
  EXPECT_EQ(r.target, "/api/x?y=1");
  EXPECT_EQ(r.version, "HTTP/1.1");
  ASSERT_NE(r.header("x-user-id"), nullptr);  // lookup is case-insensitive
  EXPECT_EQ(*r.header("x-user-id"), "42");
  EXPECT_EQ(r.header("missing"), nullptr);
  EXPECT_TRUE(r.keep_alive);
  EXPECT_TRUE(buf.empty());
}

TEST(HttpRequest, ByteByByteIsIncompleteUntilTheLastByte) {
  const std::string full = "POST /login HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc";
  std::string buf;
  HttpRequest r;
  for (size_t i = 0; i + 1 < full.size(); i++) {
    buf += full[i];
    ASSERT_EQ(parse_request(buf, r), ParseStatus::Incomplete) << "after byte " << i;
  }
  buf += full.back();
  ASSERT_EQ(parse_request(buf, r), ParseStatus::Done);
  EXPECT_EQ(r.body, "abc");
}

TEST(HttpRequest, PipelinedRequestsParseOneAtATime) {
  std::string buf = "GET /a HTTP/1.1\r\n\r\nGET /b HTTP/1.1\r\n\r\n";
  HttpRequest r;
  ASSERT_EQ(parse_request(buf, r), ParseStatus::Done);
  EXPECT_EQ(r.target, "/a");
  ASSERT_EQ(parse_request(buf, r), ParseStatus::Done);
  EXPECT_EQ(r.target, "/b");
  EXPECT_TRUE(buf.empty());
  EXPECT_EQ(parse_request(buf, r), ParseStatus::Incomplete);
}

TEST(HttpRequest, KeepAliveRules) {
  HttpRequest r;
  std::string b1 = "GET / HTTP/1.0\r\n\r\n";
  ASSERT_EQ(parse_request(b1, r), ParseStatus::Done);
  EXPECT_FALSE(r.keep_alive);  // 1.0 defaults to close
  std::string b2 = "GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n";
  ASSERT_EQ(parse_request(b2, r), ParseStatus::Done);
  EXPECT_TRUE(r.keep_alive);
  std::string b3 = "GET / HTTP/1.1\r\nConnection: close\r\n\r\n";
  ASSERT_EQ(parse_request(b3, r), ParseStatus::Done);
  EXPECT_FALSE(r.keep_alive);
}

TEST(HttpRequest, HeaderValuesAreTrimmed) {
  std::string buf = "GET / HTTP/1.1\r\nX-A:   v w  \r\n\r\n";
  HttpRequest r;
  ASSERT_EQ(parse_request(buf, r), ParseStatus::Done);
  EXPECT_EQ(*r.header("X-A"), "v w");
}

TEST(HttpRequest, RejectsTransferEncoding) {
  // Unsupported, and TE + CL together is the classic request-smuggling setup.
  HttpRequest r;
  std::string b1 = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
  EXPECT_EQ(parse_request(b1, r), ParseStatus::Error);
  std::string b2 = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 3\r\n\r\nabc";
  EXPECT_EQ(parse_request(b2, r), ParseStatus::Error);
}

TEST(HttpRequest, RejectsBadContentLength) {
  HttpRequest r;
  std::string b1 = "POST / HTTP/1.1\r\nContent-Length: abc\r\n\r\n";
  EXPECT_EQ(parse_request(b1, r), ParseStatus::Error);
  std::string b2 = "POST / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nab";
  EXPECT_EQ(parse_request(b2, r), ParseStatus::Error);
}

TEST(HttpRequest, RejectsMalformedStartLines) {
  HttpRequest r;
  for (std::string b : {"HELLO\r\n\r\n", "GET / HTTP/2.0\r\n\r\n", "GET  / HTTP/1.1\r\n\r\n",
                        "GET / HTTP/1.1\r\nNoColonHere\r\n\r\n",
                        "GET / HTTP/1.1\r\nBad Name: x\r\n\r\n"}) {
    EXPECT_EQ(parse_request(b, r), ParseStatus::Error) << b;
  }
}

TEST(HttpRequest, OversizedHeadersAreTooLarge) {
  std::string buf = "GET / HTTP/1.1\r\nX-Big: " + std::string(17 * 1024, 'a');  // no terminator yet
  HttpRequest r;
  EXPECT_EQ(parse_request(buf, r), ParseStatus::TooLarge);
}

TEST(HttpRequest, SerializeDropsHopByHopHeadersAndRoundTrips) {
  HttpRequest r;
  r.method = "POST";
  r.target = "/api/v";
  r.version = "HTTP/1.1";
  r.headers = {{"Host", "up"}, {"Connection", "close"}, {"X-User-Id", "7"}};
  r.body = "hi";
  std::string wire = serialize(r);
  EXPECT_EQ(wire.find("Connection"), std::string::npos);
  HttpRequest back;
  ASSERT_EQ(parse_request(wire, back), ParseStatus::Done);
  EXPECT_EQ(back.method, "POST");
  EXPECT_EQ(back.target, "/api/v");
  EXPECT_EQ(*back.header("x-user-id"), "7");
  EXPECT_EQ(back.body, "hi");
  EXPECT_TRUE(back.keep_alive);
}

TEST(HttpResponse, BodySplitAcrossReads) {
  std::string buf = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\no";
  HttpResponse r;
  ASSERT_EQ(parse_response(buf, r), ParseStatus::Incomplete);
  buf += "k";
  ASSERT_EQ(parse_response(buf, r), ParseStatus::Done);
  EXPECT_EQ(r.status, 200);
  EXPECT_EQ(r.body, "ok");
  EXPECT_TRUE(buf.empty());
}

TEST(HttpResponse, NoContentNeedsNoLength) {
  std::string buf = "HTTP/1.1 204 No Content\r\n\r\n";
  HttpResponse r;
  ASSERT_EQ(parse_response(buf, r), ParseStatus::Done);
  EXPECT_EQ(r.status, 204);
  EXPECT_TRUE(r.body.empty());
}

TEST(HttpResponse, SerializeRoundTrips) {
  HttpResponse r;
  r.status = 429;
  r.headers = {{"Retry-After", "3"}};
  r.body = "slow down";
  r.keep_alive = false;
  std::string wire = serialize(r);
  EXPECT_EQ(wire.rfind("HTTP/1.1 429 Too Many Requests\r\n", 0), 0u);
  EXPECT_NE(wire.find("Content-Length: 9\r\n"), std::string::npos);
  EXPECT_NE(wire.find("Connection: close\r\n"), std::string::npos);
  HttpResponse back;
  ASSERT_EQ(parse_response(wire, back), ParseStatus::Done);
  EXPECT_EQ(back.status, 429);
  EXPECT_EQ(*back.header("retry-after"), "3");
  EXPECT_EQ(back.body, "slow down");
  EXPECT_FALSE(back.keep_alive);
}
