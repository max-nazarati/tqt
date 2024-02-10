package mn.tqt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import mn.tqt.internal.KafkaReader;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.ResultMatcher;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
@WebMvcTest
class ControllerIT {

    @MockBean
    ConsumerFactory<Integer> consumerFactory;

    @MockBean
    KafkaConsumer<Integer, JsonNode> consumer;

    @MockBean
    KafkaReader<Integer, JsonNode> kafkaReader;

    ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    MockMvc mockMvc;

    @BeforeEach
    void setup() {
        when(consumerFactory.buildConsumer(any(), any())).thenReturn(consumer);
    }

    @Test
    void excludeNestedFields() throws Exception {
        ObjectNode simpleJson = (ObjectNode) objectMapper.readTree("{\"a\": {\"b\": 1}, \"c\": 2}");
        JsonNode expectedJson = objectMapper.readTree("[{\"c\": 2}]");

        Query query = simpleQuery(new QuerySchema(SchemaType.EXCLUDE, List.of("a.b")));

        when(kafkaReader.readRecords(query, consumer)).thenReturn(List.of(simpleJson));

        // ASSERTION
        String result = doRequest(query).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertEquals(objectMapper.readTree(result), expectedJson);
    }

    @Test
    void includeNestedFields() throws Exception {
        ObjectNode simpleJson = (ObjectNode) objectMapper.readTree("{\"a\": {\"b\": 1}, \"c\": {\"d\": 2}}");
        String expectedJson = "[{\"a\":{\"b\":1}}]";

        Query query = simpleQuery(new QuerySchema(SchemaType.INCLUDE, List.of("a.b")));

        when(kafkaReader.readRecords(query, consumer)).thenReturn(List.of(simpleJson));

        // ASSERTION
        MvcResult result = doRequest(query).andExpect(status().isOk()).andReturn();
        assertEquals(result.getResponse().getContentAsString(), expectedJson);
    }

    @Test
    void listOfObjects() throws Exception {
        JsonNode simpleJson = objectMapper.readTree("{\"1\": [{\"b\": 1, \"c\": 2}]}");
        JsonNode expectedJson = objectMapper.readTree("[{\"1\": [{\"b\": 1}]}]");

        Query query = simpleQuery(new QuerySchema(SchemaType.EXCLUDE, List.of("1.c")));

        when(kafkaReader.readRecords(query, consumer)).thenReturn(List.of(simpleJson));

        // ASSERTION
        String result = doRequest(query).andExpect(status().isOk()).andReturn().getResponse().getContentAsString();
        assertEquals(objectMapper.readTree(result), expectedJson);
    }

    private Query simpleQuery(QuerySchema schema) {
        return new Query("", "", "", "", "", schema);
    }

    private ResultActions doRequest(Query query) throws Exception {
        return mockMvc.perform(post("")
                .content(objectMapper.writeValueAsString(query))
                .contentType(MediaType.APPLICATION_JSON));
    }

    private ResultMatcher[] standardAssertions(JsonNode expectedJson) throws JsonProcessingException {
        return new ResultMatcher[] {status().isOk(), content().json(objectMapper.writeValueAsString(List.of(expectedJson)))};
    }
}