package yeolmok.redisstream;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;

@Slf4j
public class StreamConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    private final String consumerName;

    private final RedisTemplate<String, Object> redisTemplate;

    public StreamConsumer(String consumerName, RedisTemplate<String, Object> redisTemplate) {
        this.consumerName = consumerName;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        log.info("[{}] {}", consumerName, message.getValue());

        // XACK
        redisTemplate.opsForStream().acknowledge(RedisStreamKeys.STREAM, RedisStreamKeys.GROUP, message.getId());
    }
}
