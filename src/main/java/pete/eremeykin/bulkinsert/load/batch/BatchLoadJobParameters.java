package pete.eremeykin.bulkinsert.load.batch;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo.TestTable;

import java.io.File;
import java.util.UUID;

@Getter
@NoArgsConstructor(access = AccessLevel.PACKAGE, force = true)
@RequiredArgsConstructor
class BatchLoadJobParameters {
    private final UUID jobId;
    private final File sourcefile;
    private final ReaderType readerType;
    private final WriterType writerType;
    private final TestTable testTable;

    @Override
    public String toString() {
        String tableName = null;
        if (testTable != null) tableName = testTable.getTableName();
        return "job#%s, %s, destination: %s, source: %s".formatted(jobId, writerType, tableName, sourcefile);
    }
}
