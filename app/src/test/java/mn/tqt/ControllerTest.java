package mn.tqt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ControllerTest {

    @Mock
    ConsumerFactory<Integer> consumerFactory;

    @Mock
    KafkaConsumer<Integer, JsonNode> consumer;

    ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    Controller underTest;

    @Test
    void worksCorrectlyForNestedFields() throws JsonProcessingException {
        JsonNode simpleJson = objectMapper.readTree("{\"a\": {\"b\": 1}, \"c\": 2}");
        var records = new ConsumerRecords<>(Map.of(
                new TopicPartition("topic", 0),
                List.of(new ConsumerRecord<>("topic", 0, 0, 1, simpleJson))
        ));
        when(consumer.poll(any())).thenReturn(records);
        when(consumerFactory.buildConsumer(any(), any())).thenReturn(consumer);

        // ASSERTION
        Query query = new Query("2022",
                "2025",
                "dummyKafka",
                "dummyTopic",
                "dummyRegistry",
                new QuerySchema(SchemaType.EXCLUDE, List.of("a.b")));

        var result = underTest.readFromKafka(query);
        var x = 1;

    }

}