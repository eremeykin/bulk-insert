package pete.eremeykin.bulkinsert.load.batch;

import lombok.experimental.Delegate;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputFileItem;

import javax.sql.DataSource;

@JobScope
@BatchLoadQualifier
@Component
class BatchLoadItemWriter implements ItemWriter<InputFileItem>, InitializingBean {
    @Delegate(types = {JdbcBatchItemWriter.class, ItemWriter.class, InitializingBean.class})
    private final JdbcBatchItemWriter<InputFileItem> itemWriter;

    public BatchLoadItemWriter(DataSource dataSource) {
        this.itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        itemWriter.setSql("""
                INSERT INTO songs (id, name, artist, album_name)
                VALUES (gen_random_uuid(), :name, :artist, :albumName)""");
        itemWriter.setDataSource(dataSource);
    }

    @Override
    public void write(Chunk<? extends InputFileItem> chunk) throws Exception {
        itemWriter.write(chunk);
    }
}
