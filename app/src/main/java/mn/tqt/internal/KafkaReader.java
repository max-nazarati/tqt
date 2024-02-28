package mn.tqt.internal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import mn.tqt.ConsumerFactory;
import mn.tqt.Query;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
class KafkaReader  {

    private final Duration longTimeout = Duration.ofSeconds(1);
    private final Duration shortTimeout = Duration.ofMillis(200);
    private final ObjectMapper objectMapper;

    KafkaReader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }


    KafkaObjects executeQuery(Query query, KafkaConsumer<?, ?> consumer) {
        var kafkaObjects = new KafkaObjects(query.schema());

        var partitionInfos = consumer.partitionsFor(query.kafkaTopic());

        var partitionTimestampMap = partitionInfos.stream()
                .map(info -> new TopicPartition(query.kafkaTopic(), info.partition()))
                .collect(Collectors.toMap(partition -> partition,
                        x -> query.typedStartDate().toInstant().toEpochMilli()));

        var offsetTimestampMap = withoutNullValues(consumer.offsetsForTimes(partitionTimestampMap));

        consumer.assign(offsetTimestampMap.keySet());
        for (var entry : offsetTimestampMap.entrySet()) {
            consumer.seek(entry.getKey(), entry.getValue().offset());
        }

        var numberOfPartitions = partitionInfos.size();
        var pausedPartitions = 0;
        var batch = consumer.poll(longTimeout);
        while (pausedPartitions < numberOfPartitions) {
            if (batch.isEmpty()) {
                break;
            }
            for (var record : batch) {
                if (record.timestamp() < query.typedEndDate().toInstant().toEpochMilli()) {
                    if (record.value() == null) {
                        continue;
                    }
                    try {
                        kafkaObjects.add((ObjectNode) objectMapper.readTree(record.value().toString()));
                    } catch (JsonProcessingException | ClassCastException e) {
                        throw new RuntimeException("failed to parse record: " + record.value().toString());
                    }
                } else {
                    var partition = new TopicPartition(query.kafkaTopic(), record.partition());
                    consumer.pause(List.of(partition));
                    pausedPartitions++;
                }
            }

            batch = consumer.poll(shortTimeout);
        }
        consumer.close();

        return kafkaObjects;
    }

    private Map<TopicPartition, OffsetAndTimestamp> withoutNullValues(Map<TopicPartition, OffsetAndTimestamp> map) {
        var result = new HashMap<TopicPartition, OffsetAndTimestamp>();

        for (var entry : map.entrySet()) {
            if (entry.getValue() != null) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        
        return result;
    }
}
