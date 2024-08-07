package pete.eremeykin.bulkinsert.load.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.OutputItem;
import pete.eremeykin.bulkinsert.load.batch.CopyUtils.LinePGOutputStream;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;


@StepScope
@Component
@BatchLoadQualifier
@WriterType.CopySyncWriterQualifier
class SyncCopyItemWriter implements ItemStreamWriter<OutputItem> {
    private final DataSource dataSource;
    private final BatchLoadJobParameters jobParameters;
    private LinePGOutputStream outputStream;

    public SyncCopyItemWriter(DataSource dataSource, BatchLoadJobParameters jobParameters) {
        this.dataSource = dataSource;
        this.jobParameters = jobParameters;
    }

    @Override
    public void write(Chunk<? extends OutputItem> chunk) throws IOException {
        for (OutputItem item : chunk) {
            CopyUtils.writeItem(item, outputStream);
        }
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        String tableName = jobParameters.getTestTable().getTableName();
        try {
            this.outputStream = new LinePGOutputStream(dataSource, tableName);
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
