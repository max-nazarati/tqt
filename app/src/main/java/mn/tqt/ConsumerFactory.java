package mn.tqt;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jetbrains.annotations.NotNull;

import java.util.Properties;
import java.util.UUID;

public class ConsumerFactory {

    public static <K> KafkaConsumer<K, JsonNode> buildAvroConsumer(String kafkaServer, String schemaRegistry) {
        var props = buildDefaultProperties(kafkaServer, schemaRegistry);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    public static <K> KafkaConsumer<K, JsonNode> buildJsonConsumer(String kafkaServer, String schemaRegistry) {
        var props = buildDefaultProperties(kafkaServer, schemaRegistry);

        return new KafkaConsumer<>(props);
    }

    private static Properties buildDefaultProperties(String kafkaServer, String schemaRegistry) {
        var props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put("json.value.type", "com.fasterxml.jackson.databind.JsonNode");
        props.put("schema.registry.url", schemaRegistry);
        props.put("auto.register.schemas", false);
        props.put("use.latest.version", true);
        return props;
    }
}
