package yeolmok.redisstream.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.RedisTemplate;
import yeolmok.redisstream.RedisStreamKeys;

@Slf4j
@Configuration
public class GroupCreator {

    private static final String BUSY_GROUP = "BUSYGROUP";

    @Bean
    public ApplicationRunner createGroup(RedisTemplate<String, Object> redisTemplate) {
        return args -> {
            try {
                redisTemplate.opsForStream().createGroup(RedisStreamKeys.STREAM, RedisStreamKeys.GROUP);
            } catch (RedisSystemException e) {
                if (e.getMostSpecificCause().getMessage() != null
                        && e.getMostSpecificCause().getMessage().contains(BUSY_GROUP)) {
                    log.info("Consumer group '{}' already exists on stream '{}'", RedisStreamKeys.GROUP, RedisStreamKeys.STREAM);
                } else {
                    throw e;
                }
            }
        };
    }
}
