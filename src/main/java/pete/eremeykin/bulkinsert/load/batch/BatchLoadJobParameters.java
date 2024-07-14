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

    @Override
    public String toString() {
        return "job#%s, chunk size: %s, source: %s".formatted(jobId, chunkSize, sourcefile);
    }
}
