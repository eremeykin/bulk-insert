package pete.eremeykin.bulkinsert.load.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobExecution;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import pete.eremeykin.bulkinsert.job.util.JobLaunchingService;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo.TestTable;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ShellComponent
@RequiredArgsConstructor
class BatchLoadCommand {
    private final JobLaunchingService<BatchLoadJobParameters> jobLaunchingService;
    private final BatchLoadProperties batchLoadProperties;
    @Value("${bulkinsert.executor.max-pool-size}")
    private String threads;

    @ShellMethod(
            value = "Upload data from specified file to the database with default Spring Batch approach",
            key = {"batch-load", "bl"})
    String batchLoad(
            @ShellOption(help = "Path to the source data file") File sourceFile,
            @ShellOption(help = "Reader type", defaultValue = "REAL") ReaderType readerType,
            @ShellOption(help = "Writer type", defaultValue = "COPY_SYNC") WriterType writerType,
            @ShellOption(help = "Destination table type", defaultValue = "NO_PK") TestTable testTable
    ) throws Exception {
        if (!Files.exists(sourceFile.toPath())) {
            return "The specified source file %s does not exist".formatted(sourceFile);
        }
        BatchLoadJobParameters jobParameters = new BatchLoadJobParameters(
                UUID.randomUUID(),
                sourceFile,
                readerType,
                writerType,
                testTable
        );
        JobExecution jobExecution = jobLaunchingService.launchJob(jobParameters);
        return formatOutput(jobExecution, jobParameters);
    }

    private String formatOutput(JobExecution jobExecution, BatchLoadJobParameters jobParameters) {
        LocalDateTime endTime = jobExecution.getEndTime();
        LocalDateTime startTime = jobExecution.getStartTime();
        if (endTime == null || startTime == null) {
            return "ERROR: no startTime or endTime ";
        }
        long start = startTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        long end = endTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        TestTable tableType = jobParameters.getTestTable();
        return Stream.of(start,
                        end,
                        tableType,
                        jobParameters.getSourcefile().getName(),
                        threads,
                        batchLoadProperties.getChunkSize(),
                        jobParameters.getReaderType(),
                        jobParameters.getWriterType()
                ).map(Object::toString)
                .collect(Collectors.joining(","));
    }
}
