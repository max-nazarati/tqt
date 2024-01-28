package mn.tqt;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

@RestController
public class Controller {

    @PostMapping
    public List<ObjectNode> readFromKafka(@RequestBody Query query) {
        var consumer = buildConsumer(query.kafkaEndpoint(), query.schemaRegistry());

        var reader = new KafkaReader<>(
                consumer,
                query.kafkaTopic(),
                query.typedStartDate().toInstant().toEpochMilli(),
                query.typedEndDate().toInstant().toEpochMilli());

        List<ConsumerRecord<Integer, JsonNode>> consumerRecords = reader.readRecords();
        return consumerRecords.stream().map(record -> (ObjectNode) record.value())
                .map(json -> NodeManipulation.applySchema(json, query)).toList();
    }

    private KafkaConsumer<Integer, JsonNode> buildConsumer(
            String server,
            String schemaRegistry) {
        var props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
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

        return new KafkaConsumer<>(props);
    }

}
