package pete.eremeykin.bulkinsert.load.batch;

import lombok.experimental.Delegate;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.SynchronizedItemStreamReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputFileItem;

@StepScope
@BatchLoadQualifier
@Component
class BatchLoadItemReader implements ItemReader<InputFileItem>, InitializingBean, ItemStreamReader<InputFileItem> {
    @Delegate(types = {ItemReader.class, InitializingBean.class, ItemStreamReader.class})
    private final SynchronizedItemStreamReader<InputFileItem> itemReader;

    public BatchLoadItemReader(
            @Qualifier("batchLoadJobParametersAtSteScope")
            BatchLoadJobParameters jobParameters) {
        FlatFileItemReader<InputFileItem> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(new FileSystemResource(jobParameters.getSourcefile()));
        DefaultLineMapper<InputFileItem> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
        tokenizer.setNames("name", "artist", "albumName");
        lineMapper.setLineTokenizer(tokenizer);
        BeanWrapperFieldSetMapper<InputFileItem> objectBeanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        objectBeanWrapperFieldSetMapper.setTargetType(InputFileItem.class);
        lineMapper.setFieldSetMapper(objectBeanWrapperFieldSetMapper);
        flatFileItemReader.setLineMapper(lineMapper);
        flatFileItemReader.setSaveState(false);
        this.itemReader = new SynchronizedItemStreamReader<>();
        itemReader.setDelegate(flatFileItemReader);
    }

    @Override
    public InputFileItem read() throws Exception {
        return itemReader.read();
    }
}
