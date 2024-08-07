package pete.eremeykin.bulkinsert.load.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.load.batch.CopyUtils.LinePGOutputStream;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.UUID;

import static pete.eremeykin.bulkinsert.load.batch.CopyUtils.DELIMITER;

@TaskletLoadQualifier
@RequiredArgsConstructor
@StepScope
@Component
class CopyTasklet implements Tasklet {
    private final DataSource dataSource;
    private final BatchLoadJobParameters parameters;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String tableName = parameters.getTestTable().getTableName();
        ExecutionContext executionContext = contribution.getStepExecution().getExecutionContext();
        int linesToSkip = LineCountingPartitioner.getLinesToSkip(executionContext);
        int limit = LineCountingPartitioner.getMaxLineCount(executionContext);
        try (BufferedReader reader = new BufferedReader(new FileReader(parameters.getSourcefile()));
             LinePGOutputStream stream = new LinePGOutputStream(dataSource, tableName)) {
            reader.lines()
                    .skip(linesToSkip)
                    .limit(limit)
                    .forEach((line) -> {
                        try {
                            stream.writeLine(UUID.randomUUID() + DELIMITER + line);
                        } catch (IOException e) {
                            throw new UncheckedIOException("Unable to write line into Postgres copy stream", e);
                        }
                    });
        }
        return RepeatStatus.FINISHED;
    }
}
