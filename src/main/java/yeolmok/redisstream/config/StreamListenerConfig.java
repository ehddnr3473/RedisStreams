package yeolmok.redisstream.config;

import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import yeolmok.redisstream.StreamConsumer;

@Configuration
public class StreamListenerConfig {

    private final StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;

    private final StreamConsumer consumer;

    public StreamListenerConfig(StreamMessageListenerContainer<String, MapRecord<String, String, String>> container,
                                StreamConsumer consumer) {
        this.container = container;
        this.consumer = consumer;
    }

    @PostConstruct
    public void init() {
        container.receive(
                Consumer.from("alarm-group", "consumer-1"),
                StreamOffset.create("alarm-stream", ReadOffset.lastConsumed()),
                consumer
        );

        container.start();
    }
}
