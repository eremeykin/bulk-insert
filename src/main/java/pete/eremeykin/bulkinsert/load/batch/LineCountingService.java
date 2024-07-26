package pete.eremeykin.bulkinsert.load.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.JobScope;

import java.io.IOException;
import java.nio.file.Files;
import java.util.stream.Stream;

@JobScope
@RequiredArgsConstructor
class LineCountingService {
    private final BatchLoadJobParameters jobParameters;

    public int countLines() {
        try (Stream<String> lines = Files.lines(jobParameters.getSourcefile().toPath())) {
            return Math.toIntExact(lines.count());
        } catch (IOException e) {
            throw new IllegalArgumentException("File could not be located: " + jobParameters.getSourcefile(), e);
        }
    }
}
