package pete.eremeykin.bulkinsert.load.batch;

import lombok.experimental.Delegate;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.db.AdvancedQualifier;
import pete.eremeykin.bulkinsert.input.InputFileItem;
import pete.eremeykin.bulkinsert.util.schema.SchemaInfo;

import javax.sql.DataSource;

@StepScope
@Component
@BatchLoadQualifier
@WriterType.InsertsWriterQualifier
class InsertsItemWriter implements ItemStreamWriter<InputFileItem>, InitializingBean {
    @Delegate(types = {JdbcBatchItemWriter.class, ItemWriter.class, InitializingBean.class})
    private final JdbcBatchItemWriter<InputFileItem> itemWriter;

    public InsertsItemWriter(
            BatchLoadJobParameters jobParameters,
            DataSource defaultDataSource,
            @AdvancedQualifier DataSource advancedDataSource
    ) {
        this.itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        itemWriter.setSql("""
                        INSERT INTO %s (%s, %s, %s, %s)
                        VALUES (gen_random_uuid(), :%s, :%s, :%s)""".formatted(
                        jobParameters.getTestTable().getTableName(),
                        SchemaInfo.TestTableColumn.ID.getSqlName(),
                        SchemaInfo.TestTableColumn.NAME.getSqlName(),
                        SchemaInfo.TestTableColumn.ARTIST.getSqlName(),
                        SchemaInfo.TestTableColumn.ALBUM_NAME.getSqlName(),
                        // ID is generated
                        SchemaInfo.TestTableColumn.NAME.getBeanFieldName(),
                        SchemaInfo.TestTableColumn.ARTIST.getBeanFieldName(),
                        SchemaInfo.TestTableColumn.ALBUM_NAME.getBeanFieldName()
                )
        );
        if (jobParameters.getWriterType() == WriterType.INSERTS_ADVANCED) {
            itemWriter.setDataSource(advancedDataSource);
        } else {
            itemWriter.setDataSource(defaultDataSource);
        }
    }

    @Override
    public void write(Chunk<? extends InputFileItem> chunk) throws Exception {
        itemWriter.write(chunk);
    }
}