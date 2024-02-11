package mn.tqt;

import com.fasterxml.jackson.databind.ObjectMapper;
import mn.tqt.internal.Service;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@ExtendWith(MockitoExtension.class)
@WebMvcTest
class ControllerIT {

    ObjectMapper objectMapper = new ObjectMapper();

    @MockBean
    Service service;

    @Autowired
    MockMvc mockMvc;

    @Test
    void test() {}

    private Query simpleQuery(QuerySchema schema) {
        return new Query("", "", "", "", "", schema);
    }

    private ResultActions doRequest(Query query) throws Exception {
        return mockMvc.perform(post("")
                .content(objectMapper.writeValueAsString(query))
                .contentType(MediaType.APPLICATION_JSON));
    }
}