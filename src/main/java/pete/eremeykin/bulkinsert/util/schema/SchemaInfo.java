package pete.eremeykin.bulkinsert.util.schema;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.experimental.UtilityClass;

import java.util.Arrays;

@UtilityClass
public class SchemaInfo {

    @Getter
    @RequiredArgsConstructor
    public enum TestTable {
        NO_PK("songs"),
        PK("songs_pk"),
        ;
        private final String tableName;
    }

    @Getter
    @RequiredArgsConstructor
    public enum TestTableColumn {
        ID("id", "id", false),
        NAME("name", "name", true),
        ARTIST("artist", "artist", true),
        ALBUM_NAME("album_name", "albumName", true);

        private final String sqlName;
        private final String beanFieldName;
        private final boolean isReadable;

        public static String[] getBeanFieldNames() {
            return Arrays.stream(TestTableColumn.values())
                    .filter(TestTableColumn::isReadable)
                    .map(TestTableColumn::getBeanFieldName)
                    .toArray(String[]::new);
        }
    }
}
