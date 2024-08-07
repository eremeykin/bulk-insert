package pete.eremeykin.bulkinsert.load.batch;

import lombok.experimental.Delegate;
import lombok.experimental.UtilityClass;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.jdbc.datasource.DataSourceUtils;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;

import static org.springframework.batch.item.support.AbstractFileItemWriter.DEFAULT_LINE_SEPARATOR;
import static pete.eremeykin.bulkinsert.util.schema.SchemaInfo.TestTableColumn;

@UtilityClass
class CopyUtils {
    static final String DELIMITER = ",";
    private static final String COPY_SQL_TEMPLATE = """
            COPY %s (%s)
            FROM STDIN
            DELIMITER '%s'
            CSV;
            """.formatted("%s", TestTableColumn.getJoined(TestTableColumn::getSqlName), DELIMITER);
    private static final DelimitedLineAggregator<Object> LINE_AGGREGATOR = new DelimitedLineAggregator<>();

    static {
        BeanWrapperFieldExtractor<Object> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(SchemaInfo.TestTableColumn.getBeanFieldNames(false));
        LINE_AGGREGATOR.setFieldExtractor(fieldExtractor);
        LINE_AGGREGATOR.setDelimiter(DELIMITER);
    }

    @SuppressWarnings("unchecked")
    private static <T> LineAggregator<T> getLineAggregator() {
        return (LineAggregator<T>) LINE_AGGREGATOR;
    }

    public static <I> void writeItem(I item, LinePGOutputStream outputStream) throws IOException {
        String line = getLineAggregator().aggregate(item);
        outputStream.writeLine(line);
    }

    static final class LinePGOutputStream implements AutoCloseable {
        private final DataSourceUtilsConnection connection;
        private final PGCopyOutputStream delegate;

        LinePGOutputStream(DataSource dataSource, String tableName) throws SQLException {
            this.connection = new DataSourceUtilsConnection(dataSource);
            String copySqlCommand = COPY_SQL_TEMPLATE.formatted(tableName);
            this.delegate = new PGCopyOutputStream(connection.unwrap(PGConnection.class), copySqlCommand);
        }

        void writeLine(String line) throws IOException {
            delegate.write(line.getBytes(StandardCharsets.UTF_8));
            delegate.write(DEFAULT_LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8));
        }

        @Override
        public void close() throws IOException {
            try {
                delegate.close();
            } finally {
                connection.close();
            }
        }
    }

    private static class DataSourceUtilsConnection implements Connection {
        @Delegate
        final Connection delegate;
        private final DataSource dataSource;

        public DataSourceUtilsConnection(DataSource dataSource) {
            delegate = DataSourceUtils.getConnection(dataSource);
            this.dataSource = dataSource;
        }

        @Override
        public void close() {
            DataSourceUtils.releaseConnection(delegate, dataSource);
        }
    }
}
