package yeolmok.redisstream.config;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import yeolmok.redisstream.RedisStreamKeys;
import yeolmok.redisstream.StreamConsumer;
import yeolmok.redisstream.sse.AlarmSseService;

@Configuration
@RequiredArgsConstructor
public class StreamListenerConfig {

    private final StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;

    private final RedisTemplate<String, Object> redisTemplate;

    private final AlarmSseService alarmSseService;

    @PostConstruct
    public void init() {
        container.receive(
                Consumer.from(RedisStreamKeys.GROUP,"consumer-1"),
                StreamOffset.create(RedisStreamKeys.STREAM, ReadOffset.lastConsumed()),
                new StreamConsumer("consumer-1", redisTemplate, alarmSseService)
        );

        container.receive(
                Consumer.from(RedisStreamKeys.GROUP,"consumer-2"),
                StreamOffset.create(RedisStreamKeys.STREAM, ReadOffset.lastConsumed()),
                new StreamConsumer("consumer-2", redisTemplate, alarmSseService)
        );

        container.start();
    }
}
