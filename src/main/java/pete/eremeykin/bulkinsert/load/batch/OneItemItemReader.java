package pete.eremeykin.bulkinsert.load.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputFileItem;

@StepScope
@BatchLoadQualifier
@Component
class OneItemItemReader implements ItemReader<InputFileItem>, ItemStreamReader<InputFileItem> {
    private final InputFileItem item = new InputFileItem(
            "Waylaying Scooter", "BP Rania", "Mammoth Crocodile"
    );
    // Thread confined
    private int counter = 0;
    private int max = 0;

    @Override
    public InputFileItem read() {
        if (counter++ >= max) {
            return null;
        }
        return item;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        max = LineCountingPartitioner.getMaxLineCount(executionContext);
    }
}
