package pete.eremeykin.bulkinsert.load.batch;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.jdbc.PgConnection;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.*;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputFileItem;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.springframework.batch.item.support.AbstractFileItemWriter.DEFAULT_LINE_SEPARATOR;


@StepScope
@BatchLoadQualifier
@Primary
@Component
class AsyncCopyItemWriter implements ItemWriter<InputFileItem>, ItemStream {
    private final BlockingQueue<QueueElement> queue;
    private final Thread thread;

    private record QueueElement(Chunk<? extends InputFileItem> chunk) {
        private static final QueueElement POISONED = new QueueElement(null);
    }

    public AsyncCopyItemWriter(DataSource defaultDataSource) {
        DelimitedLineAggregator<InputFileItem> lineAggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<InputFileItem> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"name", "artist", "albumName"});
        lineAggregator.setFieldExtractor(fieldExtractor);
        this.queue = new LinkedBlockingQueue<>();
        this.thread = new BackgroundThread(defaultDataSource, queue, lineAggregator);
    }

    @Override
    public void write(Chunk<? extends InputFileItem> chunk) {
        queue.add(new QueueElement(chunk));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        thread.start();
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            queue.add(QueueElement.POISONED);
            thread.join();
        } catch (Exception e) {
            throw new ItemStreamException("Unable to join executor thread", e);
        }
    }

    @RequiredArgsConstructor
    private static class BackgroundThread extends Thread {
        private final DataSource dataSource;
        private final BlockingQueue<QueueElement> queue;
        private final LineAggregator<InputFileItem> lineAggregator;

        @Override
        public void run() {
            try (Connection connection = new DataSourceUtilsConnection();
                 PGCopyOutputStream outputStream = new PGCopyOutputStream(connection.unwrap(PgConnection.class),
                         """
                                 COPY songs (id, name, artist, album_name)
                                 FROM STDIN
                                 DELIMITER ','
                                 CSV;
                                 """)
            ) {
                while (true) {
                    QueueElement element = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (element == null) continue;
                    if (element == QueueElement.POISONED) return;
                    for (InputFileItem item : element.chunk) {
                        outputStream.write(UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
                        outputStream.write(",".getBytes(StandardCharsets.UTF_8));
                        outputStream.write(this.lineAggregator.aggregate(item).getBytes(StandardCharsets.UTF_8));
                        outputStream.write(DEFAULT_LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8));
                    }
                }
            } catch (IOException | InterruptedException | SQLException e) {
                throw new RuntimeException(e);
            }
        }

        private class DataSourceUtilsConnection extends ConnectionWrapper {

            public DataSourceUtilsConnection() {
                super(DataSourceUtils.getConnection(dataSource));
            }

            @Override
            public void close() {
                DataSourceUtils.releaseConnection(delegate, dataSource);
            }
        }
    }

    @RequiredArgsConstructor
    static class ConnectionWrapper implements Connection {
        @Delegate
        final Connection delegate;
    }
}
