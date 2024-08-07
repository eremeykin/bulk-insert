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
import pete.eremeykin.bulkinsert.input.InputItem;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo;

@JobScope
@InputGeneratorQualifier
@Component
class InputGeneratorItemWriter implements ItemWriter<InputItem>, InitializingBean, ItemStream {
    @Delegate(types = {ItemWriter.class, InitializingBean.class, ItemStream.class})
    private final FlatFileItemWriter<InputItem> itemWriter;

    public InputGeneratorItemWriter(InputGeneratorJobParameters jobParameters) {
        this.itemWriter = new FlatFileItemWriter<>();
        itemWriter.setResource(new FileSystemResource(jobParameters.getOutputFile()));
        DelimitedLineAggregator<InputItem> lineAggregator = new DelimitedLineAggregator<>();
        BeanWrapperFieldExtractor<InputItem> fieldExtractor = new BeanWrapperFieldExtractor<>();
        fieldExtractor.setNames(SchemaInfo.TestTableColumn.getBeanFieldNames(true));
        lineAggregator.setFieldExtractor(fieldExtractor);
        itemWriter.setLineAggregator(lineAggregator);
        itemWriter.setAppendAllowed(false);
    }
}
