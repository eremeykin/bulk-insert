package pete.eremeykin.bulkinsert.input.generator;

import lombok.experimental.Delegate;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputFileItem;

@JobScope
@Component
class InputGeneratorItemWriter implements ItemWriter<InputFileItem>, InitializingBean, ItemStream {
    @Delegate(types = {ItemWriter.class, InitializingBean.class, ItemStream.class})
    private final FlatFileItemWriter<InputFileItem> itemWriter;

    public InputGeneratorItemWriter(InputGeneratorJobParameters jobParameters) {
        this.itemWriter = new FlatFileItemWriter<>();
        itemWriter.setResource(new FileSystemResource(jobParameters.getOutputFile()));
        DelimitedLineAggregator<InputFileItem> lineAggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<InputFileItem> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(new String[]{"name", "artist", "albumName"});
        lineAggregator.setFieldExtractor(fieldExtractor);
        itemWriter.setLineAggregator(lineAggregator);
        itemWriter.setAppendAllowed(false);
    }
}
