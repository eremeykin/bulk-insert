package pete.eremeykin.bulkinsert.input.generator;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.JobExecution;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import pete.eremeykin.bulkinsert.job.util.JobLaunchingService;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@RequiredArgsConstructor
@ShellComponent
class InputGeneratorCommand {
    private final JobLaunchingService<InputGeneratorJobParameters> jobLaunchingService;

    @ShellMethod(
            value = "Generate input file with random data",
            key = {"generate-input-file", "gif"})
    String generateInputFile(
            @ShellOption(help = "Path to the resulting file") File file,
            @ShellOption(help = "Size of the file in lines", defaultValue = "1000000") int lines
    ) throws Exception {
        boolean fileExists = Files.exists(file.toPath());
        if (fileExists) {
            return "File %s already exists".formatted(file.getAbsolutePath());
        }
        var parameters = new InputGeneratorJobParameters(
                UUID.randomUUID(),
                file,
                lines
        );
        JobExecution jobExecution = jobLaunchingService.launchJob(parameters);
        LocalDateTime endTime = jobExecution.getEndTime();
        LocalDateTime startTime = jobExecution.getStartTime();
        String duration = "";
        if (endTime != null && startTime != null) {
            duration = " in %.2f seconds".formatted(Duration.between(startTime, endTime).toMillis() / 1000.0);
        }
        return "Job completed: %s%s".formatted(parameters, duration);
    }
}
