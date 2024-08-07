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
import pete.eremeykin.bulkinsert.input.OutputItem;

import javax.sql.DataSource;

import static pete.eremeykin.bulkinsert.util.schema.SchemaInfo.TestTableColumn;

@StepScope
@Component
@BatchLoadQualifier
@WriterType.InsertsWriterQualifier
class InsertsItemWriter implements ItemStreamWriter<OutputItem>, InitializingBean {
    @Delegate(types = {JdbcBatchItemWriter.class, ItemWriter.class, InitializingBean.class})
    private final JdbcBatchItemWriter<OutputItem> itemWriter;

    public InsertsItemWriter(
            BatchLoadJobParameters jobParameters,
            DataSource defaultDataSource,
            @AdvancedQualifier DataSource advancedDataSource
    ) {
        this.itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        itemWriter.setSql("""
                INSERT INTO %s (%s)
                VALUES (%s)""".formatted(
                        jobParameters.getTestTable().getTableName(),
                TestTableColumn.getJoined(TestTableColumn::getSqlName),
                TestTableColumn.getJoined((column) -> ":" + column.getBeanFieldName())
                )
        );
        DataSource dataSource = jobParameters.getWriterType() == WriterType.INSERTS_ADVANCED ?
                advancedDataSource
                : defaultDataSource;
        itemWriter.setDataSource(dataSource);
    }

    @Override
    public void write(Chunk<? extends OutputItem> chunk) throws Exception {
        itemWriter.write(chunk);
    }
}
