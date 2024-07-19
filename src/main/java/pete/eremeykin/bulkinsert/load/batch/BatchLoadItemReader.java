package pete.eremeykin.bulkinsert.load.batch;

import lombok.experimental.Delegate;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputFileItem;

@JobScope
@BatchLoadQualifier
@Component
class BatchLoadItemReader implements ItemReader<InputFileItem>,
        ResourceAwareItemReaderItemStream<InputFileItem>,
        InitializingBean {
    @Delegate(types = {ItemReader.class, ResourceAwareItemReaderItemStream.class, InitializingBean.class})
    private final FlatFileItemReader<InputFileItem> itemReader;

    public BatchLoadItemReader(BatchLoadJobParameters jobParameters) {
        this.itemReader = new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(jobParameters.getSourcefile()));
        DefaultLineMapper<InputFileItem> lineMapper = new DefaultLineMapper<>();
        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer(",");
        tokenizer.setNames("name", "artist", "albumName");
        lineMapper.setLineTokenizer(tokenizer);
        BeanWrapperFieldSetMapper<InputFileItem> objectBeanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        objectBeanWrapperFieldSetMapper.setTargetType(InputFileItem.class);
        lineMapper.setFieldSetMapper(objectBeanWrapperFieldSetMapper);
        itemReader.setLineMapper(lineMapper);
    }

    @Override
    public InputFileItem read() throws Exception {
        return itemReader.read();
    }
}
