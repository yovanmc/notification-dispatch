-- KEYS[1] = idempotency key (e.g. "notification:idempotency:{key}")
-- KEYS[2] = status key (e.g. "notification:status:{jobId}")
-- KEYS[3] = stream key (e.g. "notifications:jobs")
-- ARGV[1] = jobId
-- ARGV[2] = job JSON payload
-- ARGV[3] = initial status JSON
-- ARGV[4] = idempotency TTL in seconds
-- ARGV[5] = status TTL in seconds
-- ARGV[6] = request intent hash (SHA256 hex of canonical JSON over all intent fields including metadata)
--
-- Returns:
--   {0, existingJobId} if idempotency key exists with matching hash (duplicate — same request)
--   {1, jobId}         if enqueued successfully (new job)
--   {2, existingJobId} if idempotency key exists with different hash (conflict — body mismatch)

local existing = redis.call('GET', KEYS[1])
if existing then
    -- Format: "jobId|hash" (written by current version)
    -- Legacy records (written before hash support) contain only "jobId" with no "|".
    -- Treat legacy records as conflicts so they are not silently reused.
    local sep = string.find(existing, '|', 1, true)
    if sep == nil then
        return {2, existing}
    end
    local existingJobId = string.sub(existing, 1, sep - 1)
    local existingHash  = string.sub(existing, sep + 1)
    if existingHash == ARGV[6] then
        return {0, existingJobId}
    else
        return {2, existingJobId}
    end
end

-- Store idempotency record: "jobId|hash"
redis.call('SET', KEYS[1], ARGV[1] .. '|' .. ARGV[6], 'EX', ARGV[4])

-- Write initial status
redis.call('SET', KEYS[2], ARGV[3], 'EX', ARGV[5])

-- Enqueue to stream (unbounded — no MAXLEN; trimming unprocessed jobs would undermine durability)
redis.call('XADD', KEYS[3], '*', 'payload', ARGV[2])

return {1, ARGV[1]}
