package yeolmok.redisstream;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import yeolmok.redisstream.sse.AlarmSseService;

@Slf4j
@RequiredArgsConstructor
public class StreamConsumer implements StreamListener<String, MapRecord<String, String, String>> {

    private final String consumerName;

    private final RedisTemplate<String, Object> redisTemplate;

    private final AlarmSseService alarmSseService;

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        log.info("[{}] {}", consumerName, message.getValue());

        alarmSseService.broadcast(message.getValue());

        // XACK
        redisTemplate.opsForStream().acknowledge(RedisStreamKeys.STREAM, RedisStreamKeys.GROUP, message.getId());
    }
}
