package mn.tqt.local_kafka_initializer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord

class DummyController {

    fun populateKafka() {
        val props = java.util.Properties()
        props[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "localhost:9094"
        props[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] =
            "org.apache.kafka.common.serialization.StringSerializer"
        props[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] =
            "io.confluent.kafka.serializers.KafkaJsonSerializer"
        props["schema.registry.url"] = "localhost:8081"

        val producer = KafkaProducer<String, SomeObject>(props)

        val nestedObject = NestedObject(listOf(Entry(1.5), Entry(2.0)))
        for (i in 1..100) {
            val data = SomeObject(java.util.UUID.randomUUID(), listOf("a", "b"), nestedObject)
            val record = ProducerRecord("my-topic", data.id.toString(), data)
            Thread.sleep(2)
            producer.send(record)
        }


    }
}

data class SomeObject(
    val id: java.util.UUID, val strings: List<String>, val nestedObject:
    NestedObject
)

data class NestedObject(val entries: List<Entry>)

data class Entry(val entryVal1: Double?)