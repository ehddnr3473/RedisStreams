package yeolmok.redisstream;

import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class StreamProducer {

    private final RedisTemplate<String, Object> redisTemplate;

    private static final AtomicInteger counter = new AtomicInteger(0);

    public StreamProducer(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Scheduled(fixedRate = 10000)
    public void publish() {
        Map<String, String> body = Map.of(
                "message", "Counter: " + Integer.toString(counter.getAndIncrement()),
                "time", LocalDateTime.now().toString()
        );

        redisTemplate.opsForStream().add(StreamRecords.newRecord().in("alarm-stream").ofMap(body));
    }
}
