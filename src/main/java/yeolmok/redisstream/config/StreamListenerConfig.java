package yeolmok.redisstream.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import yeolmok.redisstream.RedisStreamKeys;
import yeolmok.redisstream.StreamConsumer;

@Configuration
public class StreamListenerConfig {

    private final StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;

    private final RedisTemplate<String, Object> redisTemplate;

    public StreamListenerConfig(StreamMessageListenerContainer<String, MapRecord<String, String, String>> container,
                                RedisTemplate<String, Object> redisTemplate) {
        this.container = container;
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    public void init() {
        container.receive(
                Consumer.from(RedisStreamKeys.GROUP,"consumer-1"),
                StreamOffset.create(RedisStreamKeys.STREAM, ReadOffset.lastConsumed()),
                new StreamConsumer("consumer-1", redisTemplate)
        );

        container.receive(
                Consumer.from(RedisStreamKeys.GROUP,"consumer-2"),
                StreamOffset.create(RedisStreamKeys.STREAM, ReadOffset.lastConsumed()),
                new StreamConsumer("consumer-2", redisTemplate)
        );

        container.start();
    }
}
