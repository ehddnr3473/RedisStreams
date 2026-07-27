package yeolmok.redisstream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class PendingMessageRecoverer {

    private static final String RECOVERY_CONSUMER = "consumer-recovery";

    private static final Duration MIN_IDLE_TIME = Duration.ofSeconds(30);

    private final RedisTemplate<String, Object> redisTemplate;

    @Scheduled(fixedRate = 30000)
    public void recover() {
        // XPENDING
        PendingMessages pending = redisTemplate.opsForStream()
                .pending(RedisStreamKeys.STREAM, RedisStreamKeys.GROUP, Range.unbounded(), 100, MIN_IDLE_TIME);

        if (pending.isEmpty()) {
            return;
        }

        RecordId[] ids = pending.stream()
                .map(PendingMessage::getId)
                .toArray(RecordId[]::new);

        // XCLAIM
        List<MapRecord<String, Object, Object>> claimed = redisTemplate.opsForStream()
                .claim(RedisStreamKeys.STREAM, RedisStreamKeys.GROUP, RECOVERY_CONSUMER, MIN_IDLE_TIME, ids);

        for (MapRecord<String, Object, Object> record : claimed) {
            log.warn("[{}] recovered pending message: {}", RECOVERY_CONSUMER, record.getValue());

            // XACK
            redisTemplate.opsForStream().acknowledge(RedisStreamKeys.STREAM, RedisStreamKeys.GROUP, record.getId());
        }
    }
}
