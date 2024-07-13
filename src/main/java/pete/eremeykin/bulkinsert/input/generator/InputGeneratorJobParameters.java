package pete.eremeykin.bulkinsert.input.generator;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

import java.io.File;
import java.util.UUID;

@Getter
@NoArgsConstructor(access = AccessLevel.PACKAGE, force = true)
@RequiredArgsConstructor
class InputGeneratorJobParameters {
    private final UUID jobId;
    private final File outputFile;
    private final int outputSizeInLines;

    @Override
    public String toString() {
        return "job#%s, %s lines, output: %s".formatted(jobId, outputSizeInLines, outputFile);
    }
}
