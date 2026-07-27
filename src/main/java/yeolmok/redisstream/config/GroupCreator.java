package yeolmok.redisstream.config;

import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

@Configuration
public class GroupCreator {

    @Bean
    public ApplicationRunner createGroup(RedisTemplate<String, Object> redisTemplate) {
        return args -> {
            try {
                redisTemplate.opsForStream().createGroup("alarm-stream", "alarm-group");
            } catch (Exception e) {
                // 이미 존재하는 경우 무시
            }
        };
    }
}
