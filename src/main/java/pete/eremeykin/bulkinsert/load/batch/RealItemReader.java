package pete.eremeykin.bulkinsert.load.batch;

import lombok.experimental.Delegate;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputItem;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo;

@Primary
@StepScope
@ReaderType.RealReaderQualifier
@Component
class RealItemReader implements ItemReader<InputItem>, InitializingBean, ItemStreamReader<InputItem> {
    @Delegate(types = {ItemReader.class, InitializingBean.class, ItemStreamReader.class})
    private final FlatFileItemReader<InputItem> itemReader;

    RealItemReader(BatchLoadJobParameters jobParameters) {
        FlatFileItemReader<InputItem> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new FileSystemResource(jobParameters.getSourcefile()));
        DefaultLineMapper<InputItem> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
        tokenizer.setNames(SchemaInfo.TestTableColumn.getBeanFieldNames(true));
        lineMapper.setLineTokenizer(tokenizer);
        BeanWrapperFieldSetMapper<InputItem> objectBeanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        objectBeanWrapperFieldSetMapper.setTargetType(InputItem.class);
        lineMapper.setFieldSetMapper(objectBeanWrapperFieldSetMapper);
        flatFileItemReader.setLineMapper(lineMapper);
        flatFileItemReader.setSaveState(false);
        this.itemReader = flatFileItemReader;
    }

    @Override
    public InputItem read() throws Exception {
        return itemReader.read();
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        itemReader.open(executionContext);
        itemReader.setMaxItemCount(LineCountingPartitioner.getMaxLineCount(executionContext));
        itemReader.setLinesToSkip(LineCountingPartitioner.getLinesToSkip(executionContext));
    }
}
