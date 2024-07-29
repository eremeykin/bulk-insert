package pete.eremeykin.bulkinsert.load.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import pete.eremeykin.bulkinsert.job.util.JobLaunchingService;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo.TestTable;

import java.io.File;
import java.nio.file.Files;
import java.util.UUID;

@ShellComponent
@RequiredArgsConstructor
class BatchLoadCommand {
    private final JobLaunchingService<BatchLoadJobParameters> jobLaunchingService;

    @ShellMethod(
            value = "Upload data from specified file to the database with default Spring Batch approach",
            key = {"batch-load", "bl"})
    String batchLoad(
            @ShellOption(help = "Path to the source data file") File sourceFile,
            @ShellOption(help = "Writer type", defaultValue = "COPY_SYNC") WriterType writerType,
            @ShellOption(help = "Destination table type", defaultValue = "NO_PK") TestTable testTable
    ) throws Exception {
        if (!Files.exists(sourceFile.toPath())) {
            return "The specified source file %s does not exist".formatted(sourceFile);
        }
        BatchLoadJobParameters jobParameters = new BatchLoadJobParameters(
                UUID.randomUUID(),
                sourceFile,
                writerType,
                testTable
        );
        return jobLaunchingService.launchJob(jobParameters);
    }
}
