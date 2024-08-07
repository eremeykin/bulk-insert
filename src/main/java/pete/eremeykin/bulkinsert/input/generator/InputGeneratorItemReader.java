package pete.eremeykin.bulkinsert.input.generator;

import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputItem;

import java.util.concurrent.atomic.AtomicInteger;

@JobScope
@Component
@InputGeneratorQualifier
@RequiredArgsConstructor
class InputGeneratorItemReader implements ItemReader<InputItem> {
    private final InputGeneratorJobParameters jobParameters;
    private final AtomicInteger itemsProduced = new AtomicInteger();
    private final ItemsGenerator generator;

    @Override
    public InputItem read() {
        if (itemsProduced.incrementAndGet() > jobParameters.getOutputSizeInLines()) return null;
        return generator.generateNext();
    }
}
