package pete.eremeykin.bulkinsert.load.batch;

import lombok.experimental.Delegate;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.jdbc.datasource.DataSourceUtils;
import pete.eremeykin.bulkinsert.input.InputFileItem;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

import static org.springframework.batch.item.support.AbstractFileItemWriter.DEFAULT_LINE_SEPARATOR;

final class ItemPGOutputStream<I> implements AutoCloseable {
    private static final String COPY_SQL = """
            COPY songs (%s, %s, %s, %s)
            FROM STDIN
            DELIMITER ','
            CSV;
            """.formatted(
            SchemaInfo.TestTableColumn.ID.getSqlName(),
            SchemaInfo.TestTableColumn.NAME.getSqlName(),
            SchemaInfo.TestTableColumn.ARTIST.getSqlName(),
            SchemaInfo.TestTableColumn.ALBUM_NAME.getSqlName()
    );

    private final PGCopyOutputStream delegate;
    private final DataSourceUtilsConnection connection;
    private final DelimitedLineAggregator<I> lineAggregator;

    public ItemPGOutputStream(DataSource dataSource) throws SQLException {
        this.connection = new DataSourceUtilsConnection(dataSource);
        this.lineAggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<I> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(SchemaInfo.TestTableColumn.getBeanFieldNames());
        lineAggregator.setFieldExtractor(fieldExtractor);
        this.delegate = new PGCopyOutputStream(connection.unwrap(PGConnection.class), COPY_SQL);
    }

    public void write(I item) throws IOException {
        delegate.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        delegate.write(",".getBytes(StandardCharsets.UTF_8));
        delegate.write(lineAggregator.aggregate(item).getBytes(StandardCharsets.UTF_8));
        delegate.write(DEFAULT_LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() throws Exception {
        try {
            delegate.close();
        } finally {
            connection.close();
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
