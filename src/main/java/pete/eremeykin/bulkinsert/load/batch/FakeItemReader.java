package pete.eremeykin.bulkinsert.load.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputItem;
import pete.eremeykin.bulkinsert.input.generator.ItemsGenerator;

import java.util.concurrent.atomic.AtomicLong;

@Primary
@StepScope
@ReaderType.FakeReaderQualifier
@Component
class FakeItemReader implements ItemReader<InputItem>, ItemStreamReader<InputItem> {
    private final InputItem singleItem;
    private final AtomicLong lineIndex = new AtomicLong(0);
    private int maxItemIndex;

    public FakeItemReader(ItemsGenerator itemsGenerator) {
        this.singleItem = itemsGenerator.generateNext();
    }

    @Override
    public InputItem read() {
        if (lineIndex.incrementAndGet() > maxItemIndex) {
            return null;
        }
        return singleItem;
    }


    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.maxItemIndex = LineCountingPartitioner.getMaxLineCount(executionContext);
    }
}
