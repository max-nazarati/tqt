package mn.tqt.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import mn.tqt.Query;
import mn.tqt.QuerySchema;
import mn.tqt.SchemaType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServiceTest {

    ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    KafkaReader kafkaReader;

    @InjectMocks
    Service underTest;

    @Test
    void excludeNestedFields() throws Exception {
        List<JsonNode> records = List.of(objectMapper.readTree("{\"a\": {\"b\": 1}, \"c\": 2}"));
        List<JsonNode> expectedJson = List.of(objectMapper.readTree("{\"c\": 2}"));

        Query query = simpleQuery(new QuerySchema(SchemaType.EXCLUDE, List.of("a.b")));

        when(kafkaReader.readRecords(query)).thenReturn(records);

        // ASSERTION
        assertThat(expectedJson).isEqualTo(underTest.readRecordsWithSchema(query));
    }

    @Test
    void includeNestedFields() throws Exception {
        List<JsonNode> records = List.of(objectMapper.readTree("{\"a\": {\"b\": 1}, \"c\": {\"d\": 2}}"));
        List<JsonNode> expectedJson = List.of(objectMapper.readTree("{\"a\":{\"b\":1}}"));

        Query query = simpleQuery(new QuerySchema(SchemaType.INCLUDE, List.of("a.b")));

        when(kafkaReader.readRecords(query)).thenReturn(records);

        // ASSERTION
        assertThat(expectedJson).isEqualTo(underTest.readRecordsWithSchema(query));
    }

    @Test
    void listOfObjects() throws Exception {
        List<JsonNode> records = List.of(objectMapper.readTree("{\"1\": [{\"b\": 1, \"c\": 2}]}"));
        List<JsonNode> expectedJson = List.of(objectMapper.readTree("{\"1\": [{\"b\": 1}]}"));

        Query query = simpleQuery(new QuerySchema(SchemaType.EXCLUDE, List.of("1.c")));

        when(kafkaReader.readRecords(query)).thenReturn(records);

        // ASSERTION
        assertThat(expectedJson).isEqualTo(underTest.readRecordsWithSchema(query));
    }

    private Query simpleQuery(QuerySchema schema) {
        return new Query("", "", "", "", "", schema);
    }


}