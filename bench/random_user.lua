-- wrk script: every request comes from one of 10,000 users, so the keys spread
-- over both Redis shards (and, in hybrid mode, each user needs its own lease).
request = function()
  return wrk.format(nil, nil, { ["X-User-Id"] = "user" .. math.random(10000) })
end
