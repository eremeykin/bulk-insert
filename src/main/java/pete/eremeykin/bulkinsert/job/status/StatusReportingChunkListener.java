package pete.eremeykin.bulkinsert.job.status;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;

@Slf4j
public class StatusReportingChunkListener implements ChunkListener {

    @Override
    public void afterChunk(ChunkContext context) {
        log.info(
                "Lines written: {}",
                context.getStepContext().getStepExecution().getWriteCount()
        );
    }

}
