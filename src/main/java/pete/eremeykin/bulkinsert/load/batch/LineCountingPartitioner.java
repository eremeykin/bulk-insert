package pete.eremeykin.bulkinsert.load.batch;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

@RequiredArgsConstructor
class LineCountingPartitioner implements Partitioner {
    private static final String LINES_TO_SKIP_KEY = "linesToSkip";
    private static final String MAX_LINE_COUNT_KEY = "maxLineCount";
    private static final String PARTITION_PREFIX = "partition-";

    private final LineCountingService lineCountingService;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        int lineCount = lineCountingService.countLines();
        if (lineCount == 0) return emptyMap();
        int remainder = lineCount % gridSize;
        int minimalLinesToRead = lineCount / gridSize;
        Map<String, ExecutionContext> result = new HashMap<>();
        for (int i = 0, startFrom = 0; i < gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt(LINES_TO_SKIP_KEY, startFrom);
            int linesToRead = minimalLinesToRead;
            if (remainder > 0) {
                linesToRead += 1;
                remainder--;
            }
            context.putInt(MAX_LINE_COUNT_KEY, linesToRead);
            startFrom = startFrom + linesToRead;
            result.put(PARTITION_PREFIX + i, context);
        }
        return result;
    }


    public static int getLinesToSkip(ExecutionContext executionContext) {
        return executionContext.getInt(LINES_TO_SKIP_KEY);
    }

    public static int getMaxLineCount(ExecutionContext executionContext) {
        return executionContext.getInt(MAX_LINE_COUNT_KEY);
    }
}
