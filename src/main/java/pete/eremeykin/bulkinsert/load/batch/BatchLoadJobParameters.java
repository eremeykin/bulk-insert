package pete.eremeykin.bulkinsert.load.batch;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.io.File;
import java.util.UUID;

@Getter
@NoArgsConstructor(access = AccessLevel.PACKAGE, force = true)
@RequiredArgsConstructor
class BatchLoadJobParameters {
    private final UUID jobId;
    private final File sourcefile;
    private final int chunkSize;
    private final boolean advancedDataSource;

    @Override
    public String toString() {
        String dataSource = "default";
        if (advancedDataSource) dataSource = "advanced";
        return "job#%s, data source: %s, chunk size: %s, source: %s".formatted(jobId, dataSource, chunkSize, sourcefile);
    }
}
