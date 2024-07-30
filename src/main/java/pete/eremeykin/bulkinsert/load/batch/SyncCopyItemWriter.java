package pete.eremeykin.bulkinsert.load.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputFileItem;
import pete.eremeykin.bulkinsert.load.batch.CopyUtils.ItemPGOutputStream;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;


@StepScope
@Component
@RequiredArgsConstructor
@BatchLoadQualifier
@WriterType.CopySyncWriterQualifier
class SyncCopyItemWriter implements ItemStreamWriter<InputFileItem> {
    private final DataSource dataSource;
    private final BatchLoadJobParameters jobParameters;
    private ItemPGOutputStream<InputFileItem> outputStream;

    @Override
    public void write(Chunk<? extends InputFileItem> chunk) throws IOException {
        for (InputFileItem item : chunk) {
            this.outputStream.write(item);
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        String tableName = jobParameters.getTestTable().getTableName();
        try {
            this.outputStream = new ItemPGOutputStream<>(dataSource, tableName);
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
