package pete.eremeykin.bulkinsert.load.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.*;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputFileItem;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;


@StepScope
@Component
@BatchLoadQualifier
@WriterType.CopySyncWriterQualifier
class SyncCopyItemWriter implements ItemWriter<InputFileItem>, ItemStream {
    private final DataSource dataSource;
    private ItemPGOutputStream<InputFileItem> outputStream;

    public SyncCopyItemWriter(DataSource defaultDataSource) {
        this.dataSource = defaultDataSource;
    }

    @Override
    public void write(Chunk<? extends InputFileItem> chunk) throws IOException {
        for (InputFileItem item : chunk) {
            this.outputStream.write(item);
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            this.outputStream = new ItemPGOutputStream<>(dataSource);
        } catch (SQLException e) {
            throw new ItemStreamException("Unable to open Postgres copy stream", e);
        }
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            this.outputStream.close();
        } catch (Exception e) {
            throw new ItemStreamException("Unable to close " + this.getClass().getName(), e);
        }
    }
}
