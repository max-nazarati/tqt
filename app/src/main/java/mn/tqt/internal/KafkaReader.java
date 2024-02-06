package mn.tqt.internal;

import mn.tqt.Query;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class KafkaReader<K, V>  {

    private static final Duration longTimeout = Duration.ofSeconds(2);
    private static final Duration shortTimeout = Duration.ofMillis(200);

    public List<ConsumerRecord<K, V>> readRecords(Query query, KafkaConsumer<K, V> consumer) {

        var acc = new ArrayList<ConsumerRecord<K, V>>();

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
                    acc.add(record);
                } else {
                    var partition = new TopicPartition(query.kafkaTopic(), record.partition());
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
