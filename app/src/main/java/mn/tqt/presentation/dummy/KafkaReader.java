package mn.tqt.presentation.dummy;

import org.apache.kafka.common.TopicPartition;

import java.time.Duration;

public class KafkaReader<K, V> {
    private final org.apache.kafka.clients.consumer.KafkaConsumer<K, V> consumer;
    private final String topic;
    private final Long startInstant;
    private final Long endInstant;

    private static final java.time.Duration longTimeout = java.time.Duration.ofSeconds(2);
    private static final java.time.Duration shortTimeout = java.time.Duration.ofMillis(200);

    public KafkaReader(org.apache.kafka.clients.consumer.KafkaConsumer<K, V> consumer,
                       String topic, Long startInstant, Long endInstant) {
        this.consumer = consumer;
        this.topic = topic;
        this.startInstant = startInstant;
        this.endInstant = endInstant;
    }

    public java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<K, V>> readRecords() {

        var acc = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<K, V>>();

        var partitionInfos = consumer.partitionsFor(topic);

        var partitionTimestampMap = partitionInfos.stream()
                .map(info -> new org.apache.kafka.common.TopicPartition(topic, info.partition()))
                .collect(java.util.stream.Collectors.toMap(partition -> partition,
                        x -> startInstant));

        var offsetTimestampMap = consumer.offsetsForTimes(partitionTimestampMap);

        consumer.unsubscribe();
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
                    consumer.pause(java.util.List.of(partition));
                    pausedPartitions++;
                }
            }

            batch = consumer.poll(shortTimeout);
        }

        return acc;
    }
}
