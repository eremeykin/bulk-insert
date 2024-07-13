package pete.eremeykin.bulkinsert.input.generator;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import pete.eremeykin.bulkinsert.job.parameters.converter.JobParametersConverter;

import java.io.File;
import java.nio.file.Files;
import java.util.UUID;

@RequiredArgsConstructor
@ShellComponent
class InputGeneratorCommand {
    private final JobLauncher jobLauncher;
    private final Job inputFileGenerationJob;
    private final JobParametersConverter<InputGeneratorJobParameters> jobParametersConverter;

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
        InputGeneratorJobParameters parameters = new InputGeneratorJobParameters(
                UUID.randomUUID(),
                file,
                lines
        );
        JobParameters jobParameters = jobParametersConverter.parametersFromObject(parameters);
        jobLauncher.run(inputFileGenerationJob, jobParameters);
        return "Job completed: " + parameters;
    }
}
