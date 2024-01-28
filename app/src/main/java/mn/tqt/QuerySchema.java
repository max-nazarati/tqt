package mn.tqt;

import java.util.LinkedList;
import java.util.List;

public record QuerySchema(SchemaType type, List<String> schema) {

    public java.util.List<LinkedList<String>> asListOfQueues() {
        return schema.stream().map(entry -> new LinkedList<>(List.of(entry.split("\\.")))).toList();
    }
}
