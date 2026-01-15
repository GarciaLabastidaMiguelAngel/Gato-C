package com.example.demo;


import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;

@Service
public class AccountPriorityGate {

  public enum Priority { ONLINE, BATCH }

  private final RedisTemplate<String, String> redis;

  private final long big;
  private final Duration leaseTtl;
  private final Duration waitKeyTtl;
  private final Duration rqTtlMargin;

  // Service-time estimate (ms) used to compute ETA from rank
  private final long serviceTimeMs;

  // Dynamic BLPOP chunk tuning
  private final long minChunkMs;
  private final long midChunkMs;
  private final long maxChunkMs;
  private final long etaMidMs;

  // Lua scripts
  private final DefaultRedisScript<List> enqueueWithRank;
  private final DefaultRedisScript<Long> releaseAndWake;
  private final DefaultRedisScript<Long> cancelScript;

  public AccountPriorityGate(
      RedisTemplate<String, String> redis,
      @Value("${app.gate.big}") long big,
      @Value("${app.gate.lease-ttl-ms}") long leaseTtlMs,
      @Value("${app.gate.waitkey-ttl-ms}") long waitKeyTtlMs,
      @Value("${app.gate.rq-ttl-margin-ms}") long rqTtlMarginMs,
      @Value("${app.gate.service-time-ms}") long serviceTimeMs,
      @Value("${app.gate.min-chunk-ms}") long minChunkMs,
      @Value("${app.gate.mid-chunk-ms}") long midChunkMs,
      @Value("${app.gate.max-chunk-ms}") long maxChunkMs,
      @Value("${app.gate.eta-mid-ms}") long etaMidMs
  ) {
    this.redis = redis;
    this.big = big;

    this.leaseTtl = Duration.ofMillis(leaseTtlMs);
    this.waitKeyTtl = Duration.ofMillis(waitKeyTtlMs);
    this.rqTtlMargin = Duration.ofMillis(rqTtlMarginMs);

    this.serviceTimeMs = serviceTimeMs;
    this.minChunkMs = minChunkMs;
    this.midChunkMs = midChunkMs;
    this.maxChunkMs = maxChunkMs;
    this.etaMidMs = etaMidMs;

    this.enqueueWithRank = new DefaultRedisScript<>();
    this.enqueueWithRank.setResultType(List.class);
    this.enqueueWithRank.setScriptText(LUA_ENQUEUE_WITH_RANK);

    this.releaseAndWake = new DefaultRedisScript<>();
    this.releaseAndWake.setResultType(Long.class);
    this.releaseAndWake.setScriptText(LUA_RELEASE_AND_WAKE_SKIP_ZOMBIES);

    this.cancelScript = new DefaultRedisScript<>();
    this.cancelScript.setResultType(Long.class);
    this.cancelScript.setScriptText(LUA_CANCEL_SKIP_ZOMBIES);
  }

  /**
   * Acquire exclusive execution turn for an account.
   *
   * channelDeadline: align with HTTP channel lifetime (gateway/client timeout).
   * No internal "business timeout" here—only channel-driven deadline.
   */
  @SuppressWarnings("unchecked")
  public Permit acquire(String accountId, Priority priority, Duration channelDeadline) {
    final String requestId = UUID.randomUUID().toString();

    final String qKey = queueKey(accountId);
    final String leaseKey = leaseKey(accountId);
    final String seqKey = seqKey(accountId);
    final String waitKey = waitKey(requestId);

    final long prio = (priority == Priority.ONLINE) ? 0L : 1L;

    // Marker TTL: channel lifetime + margin (auto-clean if client cuts)
    final Duration rqTtl = channelDeadline.plus(rqTtlMargin);

    List<Long> res = (List<Long>) redis.execute(
        enqueueWithRank,
        List.of(qKey, leaseKey, seqKey),
        requestId,
        String.valueOf(prio),
        String.valueOf(big),
        String.valueOf(leaseTtl.toMillis()),
        String.valueOf(rqTtl.toMillis())
    );

    long goFlag = (res != null && res.size() > 0 && res.get(0) != null) ? res.get(0) : 0L;
    long rank = (res != null && res.size() > 1 && res.get(1) != null) ? res.get(1) : 0L;

    if (goFlag == 1L) {
      return new Permit(accountId, requestId);
    }

    final long endNanos = System.nanoTime() + channelDeadline.toNanos();

    // Recalculate rank periodically to adjust chunk (avoid extra Redis calls)
    long nextRecalcAtNanos = System.nanoTime(); // recalc immediately first time
    long chunkMs = computeChunkMs(rank);

    while (System.nanoTime() < endNanos) {

      // Recalculate rank every ~1s (not each loop)
      if (System.nanoTime() >= nextRecalcAtNanos) {
        Long r = redis.opsForZSet().rank(qKey, requestId);
        if (r != null) {
          rank = r;
          chunkMs = computeChunkMs(rank);
        }
        nextRecalcAtNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
      }

      // Wait in dynamic chunks, but never beyond remaining channel time
      long remainingMs = Math.max(1, TimeUnit.NANOSECONDS.toMillis(endNanos - System.nanoTime()));
      long thisChunkMs = Math.min(chunkMs, remainingMs);

      String msg = redis.opsForList().leftPop(waitKey, Duration.ofMillis(thisChunkMs));
      if ("GO".equals(msg)) {
        String owner = redis.opsForValue().get(leaseKey);
        if (requestId.equals(owner)) {
          return new Permit(accountId, requestId);
        }
      }

      // Rare recovery: wake-up lost. If I'm head and lease is free, attempt to grab it.
      String head = zsetHead(qKey);
      if (requestId.equals(head)) {
        Boolean ok = redis.opsForValue().setIfAbsent(leaseKey, requestId, leaseTtl);
        if (Boolean.TRUE.equals(ok)) {
          return new Permit(accountId, requestId);
        }
      }
    }

    // Channel deadline exceeded: best-effort cancel to avoid functional zombies
    cancel(accountId, requestId);
    throw new RuntimeException("Channel deadline exceeded while waiting for turn. accountId=" + accountId);
  }

  public void cancel(String accountId, String requestId) {
    String qKey = queueKey(accountId);
    String leaseKey = leaseKey(accountId);
    String seqKey = seqKey(accountId);

    redis.execute(
        cancelScript,
        List.of(qKey, leaseKey, seqKey),
        requestId,
        String.valueOf(leaseTtl.toMillis()),
        String.valueOf(waitKeyTtl.toMillis())
    );

    // best-effort cleanup
    redis.delete(waitKey(requestId));
    redis.delete(rqKey(requestId));
  }

  public final class Permit implements AutoCloseable {
    private final String accountId;
    private final String requestId;
    private boolean released = false;

    Permit(String accountId, String requestId) {
      this.accountId = accountId;
      this.requestId = requestId;
    }

    public String requestId() { return requestId; }

    @Override
    public void close() {
      if (released) return;
      released = true;

      String qKey = queueKey(accountId);
      String leaseKey = leaseKey(accountId);
      String seqKey = seqKey(accountId);

      redis.execute(
          releaseAndWake,
          List.of(qKey, leaseKey, seqKey),
          requestId,
          String.valueOf(leaseTtl.toMillis()),
          String.valueOf(waitKeyTtl.toMillis())
      );

      // best-effort cleanup
      redis.delete(waitKey(requestId));
      redis.delete(rqKey(requestId));
    }
  }

  private long computeChunkMs(long rankAhead) {
    long etaMs = Math.max(0, rankAhead) * serviceTimeMs;

    // Small backlog => low latency chunks
    if (etaMs < 500) return minChunkMs;

    // Medium backlog
    if (etaMs < etaMidMs) return midChunkMs;

    // Large backlog => fewer Redis roundtrips
    return maxChunkMs;
  }

  private String zsetHead(String qKey) {
    Set<String> heads = redis.opsForZSet().range(qKey, 0, 0);
    if (heads == null || heads.isEmpty()) return null;
    return heads.iterator().next();
  }

  // Key conventions (hash tags for cluster slot stability)
  private static String queueKey(String accountId) { return "acct:{" + accountId + "}:q"; }
  private static String leaseKey(String accountId) { return "acct:{" + accountId + "}:lease"; }
  private static String seqKey(String accountId)   { return "acct:{" + accountId + "}:seq"; }
  private static String waitKey(String requestId)  { return "wait:{" + requestId + "}"; }
  private static String rqKey(String requestId)    { return "rq:{" + requestId + "}"; }

  // --------------------------
  // Lua scripts
  // --------------------------

  // KEYS[1]=qKey KEYS[2]=leaseKey KEYS[3]=seqKey
  // ARGV[1]=requestId ARGV[2]=priority ARGV[3]=BIG ARGV[4]=leaseTtlMs ARGV[5]=rqTtlMs
  // Returns {goFlag, rank}
  private static final String LUA_ENQUEUE_WITH_RANK = """
  local qKey     = KEYS[1]
  local leaseKey = KEYS[2]
  local seqKey   = KEYS[3]

  local requestId = ARGV[1]
  local priority  = tonumber(ARGV[2])
  local BIG       = tonumber(ARGV[3])
  local leaseTtl  = ARGV[4]
  local rqTtl     = ARGV[5]

  redis.call('SET', 'rq:{'..requestId..'}', '1', 'PX', rqTtl)

  local seq = redis.call('INCR', seqKey)
  local score = (priority * BIG) + seq
  redis.call('ZADD', qKey, score, requestId)

  local rank = redis.call('ZRANK', qKey, requestId) or 0

  local headArr = redis.call('ZRANGE', qKey, 0, 0)
  local head = headArr[1]
  if head == requestId then
    local ok = redis.call('SET', leaseKey, requestId, 'NX', 'PX', leaseTtl)
    if ok then
      return {1, rank}
    end
  end

  return {0, rank}
  """;

  // KEYS[1]=qKey KEYS[2]=leaseKey KEYS[3]=seqKey
  // ARGV[1]=requestId ARGV[2]=leaseTtlMs ARGV[3]=waitKeyTtlMs
  private static final String LUA_RELEASE_AND_WAKE_SKIP_ZOMBIES = """
  local qKey     = KEYS[1]
  local leaseKey = KEYS[2]
  local seqKey   = KEYS[3]

  local requestId = ARGV[1]
  local leaseTtl  = ARGV[2]
  local waitTtl   = ARGV[3]

  if redis.call('GET', leaseKey) ~= requestId then
    return -1
  end

  redis.call('ZREM', qKey, requestId)
  redis.call('DEL', leaseKey)
  redis.call('DEL', 'rq:{'..requestId..'}')

  while true do
    local nextArr = redis.call('ZRANGE', qKey, 0, 0)
    local next = nextArr[1]

    if not next then
      redis.call('DEL', qKey)
      redis.call('DEL', seqKey)
      return 0
    end

    if redis.call('EXISTS', 'rq:{'..next..'}') == 1 then
      redis.call('SET', leaseKey, next, 'PX', leaseTtl)
      local waitKey = 'wait:{'..next..'}'
      redis.call('LPUSH', waitKey, 'GO')
      redis.call('PEXPIRE', waitKey, waitTtl)
      return 1
    else
      redis.call('ZREM', qKey, next)
    end
  end
  """;

  // KEYS[1]=qKey KEYS[2]=leaseKey KEYS[3]=seqKey
  // ARGV[1]=requestId ARGV[2]=leaseTtlMs ARGV[3]=waitKeyTtlMs
  private static final String LUA_CANCEL_SKIP_ZOMBIES = """
  local qKey     = KEYS[1]
  local leaseKey = KEYS[2]
  local seqKey   = KEYS[3]

  local requestId = ARGV[1]
  local leaseTtl  = ARGV[2]
  local waitTtl   = ARGV[3]

  redis.call('ZREM', qKey, requestId)
  redis.call('DEL', 'rq:{'..requestId..'}')
  redis.call('DEL', 'wait:{'..requestId..'}')

  if redis.call('GET', leaseKey) == requestId then
    redis.call('DEL', leaseKey)

    while true do
      local nextArr = redis.call('ZRANGE', qKey, 0, 0)
      local next = nextArr[1]

      if not next then
        redis.call('DEL', qKey)
        redis.call('DEL', seqKey)
        return 1
      end

      if redis.call('EXISTS', 'rq:{'..next..'}') == 1 then
        redis.call('SET', leaseKey, next, 'PX', leaseTtl)
        local waitKey = 'wait:{'..next..'}'
        redis.call('LPUSH', waitKey, 'GO')
        redis.call('PEXPIRE', waitKey, waitTtl)
        return 1
      else
        redis.call('ZREM', qKey, next)
      end
    end
  end

  return 1
  """;
}
