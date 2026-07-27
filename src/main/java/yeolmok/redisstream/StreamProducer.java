package yeolmok.redisstream;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.connection.RedisStreamCommands.XAddOptions;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
public class StreamProducer {

    private static final long MAXLEN = 1000;

    private final RedisTemplate<String, Object> redisTemplate;

    private static final AtomicInteger counter = new AtomicInteger(0);

    @Scheduled(fixedRate = 10000)
    public void publish() {
        Map<String, String> body = Map.of(
                "message", "Counter: " + counter.getAndIncrement(),
                "time", LocalDateTime.now().toString()
        );

        // XADD
        redisTemplate.opsForStream().add(
                StreamRecords.newRecord().in(RedisStreamKeys.STREAM).ofMap(body),
                XAddOptions.maxlen(MAXLEN).approximateTrimming(true)
        );
    }
}
