package mn.tqt.internal;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import mn.tqt.Query;
import mn.tqt.QuerySchema;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

final class NodeManipulation {

    private NodeManipulation() {
    }

    static ObjectNode applySchema(ObjectNode json, Query query) {

        List<JsonPointer> pointers = switch (query.schema().type()) {
            case EXCLUDE -> constructSchemaPointers(json.deepCopy(), query.schema());
            case INCLUDE -> {
                List<JsonPointer> schemaPointers =
                        constructSchemaPointers(json.deepCopy(), query.schema());
                List<JsonPointer> allPointers = constructAllJsonPointers(json,
                        JsonPointer.compile("/"));

                yield allPointers.stream()
                        .filter(pointer -> schemaPointers.stream()
                                .noneMatch(schemaPointer -> {
                                    if (pointer.length() <= schemaPointer.length()) {
                                        return schemaPointer.toString().startsWith(pointer.toString());
                                    } else {
                                        return pointer.toString().startsWith(schemaPointer.toString());
                                    }
                                }))
                        .sorted((l, r) -> Integer.compare(pointerDepth(r), pointerDepth(l)))
                        .toList();
            }
        };

        var jsonCopy = json.deepCopy();
        for (var pointer : pointers) {
            var currentObject = (ObjectNode) jsonCopy.at(pointer.head());
            currentObject.remove(pointer.last().getMatchingProperty());
        }

        return jsonCopy;
    }

    private static int pointerDepth(JsonPointer pointer) {
        return pointer.toString().split("/").length;
    }


    /**
     Constructs JSON pointers for all the possible paths within the {@link JsonNode}
     */
    private static List<JsonPointer> constructAllJsonPointers(JsonNode json, JsonPointer pointer) {
        var jsonCopy = json.deepCopy();
        var acc = new ArrayList<JsonPointer>();

        if (jsonCopy.isValueNode()) {
            return List.of(pointer);
        } else if (jsonCopy.isObject()) {
            for (var property : jsonCopy.properties()) {

                acc.addAll(constructAllJsonPointers(property.getValue(), pointer.appendProperty(property.getKey())));
            }
        } else if (jsonCopy.isArray()) {
            for (int i = 0; i < jsonCopy.size(); i++) {
                acc.addAll(constructAllJsonPointers(jsonCopy.get(i), pointer.appendIndex(i)));
            }
        }

        return acc;
    }

    /**
    Constructs JSON pointers for all the paths specified in the {@link mn.tqt.QuerySchema}
     */
    private static List<JsonPointer> constructSchemaPointers(ObjectNode json, QuerySchema schema) {
        var acc = new ArrayList<JsonPointer>();
        for (var path : schema.asListOfQueues()) {
            acc.addAll(constructSchemaPointersForPath(json, JsonPointer.compile("/"), path));
        }
        return acc;
    }

    /**
     * Constructs JSON pointers for a particular path of {@link mn.tqt.QuerySchema}.
     * A Path can produce multiple pointers if it contains an array.
     *
     * i.e.: <pre>a.array.b</pre> would result in <pre>[a.array[0].b, a.array[1].b, ...]</pre>
     * @param json the current node. It is being traversed by the recursion.
     * @param pointer the cumulative pointer. It is being build by the recursion.
     * @param path the {@link LinkedList} representation of a single {@link mn.tqt.QuerySchema} entry
     *             The LinkedList helps with the iteration because I can just pop from the beginning.
     *             (Java does not have simple Queue implementation...)
     * @return A {@link List} of {@link JsonPointer}s that result from this particular path.
     */
    private static List<JsonPointer> constructSchemaPointersForPath(
            JsonNode json,
            JsonPointer pointer,
            LinkedList<String> path) {
        var acc = new ArrayList<JsonPointer>();
        while (!path.isEmpty()) {
            var subPath = path.pop();

            json = json.get(subPath);
            if (path.isEmpty()) {
                pointer = pointer.appendProperty(subPath);
                acc.add(pointer);
            } else if (json.isObject()) {
                pointer = pointer.appendProperty(subPath);
            } else if (json.isArray()) {

                var arrayPointer = pointer.appendProperty(subPath);
                for (int i = 0; i < json.size(); i++) {
                    // path needs to be cloned to not dirty the context of other recursions in the enclosing loop
                    // TODO: maybe using a normal list instead is less finicky
                    var arrayPointers = constructSchemaPointersForPath(json.deepCopy(),
                            arrayPointer.appendIndex(i),
                            (LinkedList<String>) path.clone());
                    acc.addAll(arrayPointers);
                }

                path.clear();
            }
        }

        return acc;
    }

}
