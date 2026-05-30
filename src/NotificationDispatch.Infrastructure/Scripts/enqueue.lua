-- KEYS[1] = idempotency key (e.g. "notification:idempotency:{key}")
-- KEYS[2] = status key (e.g. "notification:status:{jobId}")
-- KEYS[3] = stream key (e.g. "notifications:jobs")
-- ARGV[1] = jobId
-- ARGV[2] = job JSON payload
-- ARGV[3] = initial status JSON
-- ARGV[4] = idempotency TTL in seconds
-- ARGV[5] = status TTL in seconds
--
-- Returns:
--   {0, existingJobId} if idempotency key already exists
--   {1, jobId} if enqueued successfully

local existing = redis.call('GET', KEYS[1])
if existing then
    return {0, existing}
end

-- Set idempotency key -> jobId with TTL
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[4])

-- Write initial status
redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[5])

-- Enqueue to stream
-- MAXLEN ~ 10000 trims the stream to approximately 10,000 entries.
-- The '~' prefix allows Redis to trim lazily for efficiency.
redis.call('XADD', KEYS[3], 'MAXLEN', '~', '10000', '*', 'payload', ARGV[2])

return {1, ARGV[1]}
