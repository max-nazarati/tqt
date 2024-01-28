package mn.tqt;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class KafkaReader<K, V> {
    private final KafkaConsumer<K, V> consumer;
    private final String topic;
    private final Long startInstant;
    private final Long endInstant;

    private static final Duration longTimeout = Duration.ofSeconds(2);
    private static final Duration shortTimeout = Duration.ofMillis(200);

    KafkaReader(KafkaConsumer<K, V> consumer,
                       String topic, Long startInstant, Long endInstant) {
        this.consumer = consumer;
        this.topic = topic;
        this.startInstant = startInstant;
        this.endInstant = endInstant;
    }

    List<ConsumerRecord<K, V>> readRecords() {

        var acc = new ArrayList<ConsumerRecord<K, V>>();

        var partitionInfos = consumer.partitionsFor(topic);

        var partitionTimestampMap = partitionInfos.stream()
                .map(info -> new TopicPartition(topic, info.partition()))
                .collect(Collectors.toMap(partition -> partition,
                        x -> startInstant));

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
                if (record.timestamp() < endInstant) {
                    acc.add(record);
                } else {
                    var partition = new TopicPartition(topic, record.partition());
                    consumer.pause(List.of(partition));
                    pausedPartitions++;
                }
            }

            batch = consumer.poll(shortTimeout);
        }

        return acc;
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
