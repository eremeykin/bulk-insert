package pete.eremeykin.bulkinsert.input.generator;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;

import java.io.File;
import java.util.Map;
import java.util.UUID;

@RequiredArgsConstructor
@ShellComponent
class InputGeneratorCommand {
    private final JobLauncher jobLauncher;
    private final Job inputFileGenerationJob;

    @ShellMethod(
            value = "Generate input file with random data",
            key = {"generate-input-file", "gif"})
    String generateInputFile(
            @ShellOption(help = "Path to the resulting file") File file,
            @ShellOption(help = "Size of the file in lines") int lines
    ) throws Exception {
        JobParameters jobParameters = new JobParameters(
                Map.of(
                        "jobId", new JobParameter<>(UUID.randomUUID(), UUID.class, true)
                )
        );
        jobLauncher.run(inputFileGenerationJob, jobParameters);
        return "file to generate: " + file;
    }
}
