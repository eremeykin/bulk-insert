package pete.eremeykin.bulkinsert.input.generator;

import lombok.RequiredArgsConstructor;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import pete.eremeykin.bulkinsert.job.util.JobLaunchingService;

import java.io.File;
import java.nio.file.Files;
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
        return jobLaunchingService.launchJob(parameters);
    }
}
