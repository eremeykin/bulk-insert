package pete.eremeykin.bulkinsert.util.schema;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;

@UtilityClass
public class SchemaInfo {

    public static String TEST_TABLE_NAME = "songs";

    @Getter
    @RequiredArgsConstructor
    public enum TestTableColumn {
        ID("id", "id"),
        NAME("name", "name"),
        ARTIST("artist", "artist"),
        ALBUM_NAME("album_name", "albumName");

        private final String sqlName;
        private final String fieldName;
    }


}
