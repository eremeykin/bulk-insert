package pete.eremeykin.bulkinsert.load.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.InputFileItem;
import pete.eremeykin.bulkinsert.load.batch.CopyUtils.ItemPGOutputStream;

import javax.sql.DataSource;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static pete.eremeykin.bulkinsert.load.batch.WriterType.CopyAsyncWriterQualifier;


@StepScope
@Component
@BatchLoadQualifier
@CopyAsyncWriterQualifier
class AsyncCopyItemWriter implements ItemStreamWriter<InputFileItem> {
    private final BlockingQueue<QueueElement> queue;
    private final FutureTask<Void> backgroundTask;
    private final AsyncTaskExecutor taskExecutor;

    private record QueueElement(Chunk<? extends InputFileItem> chunk) {
        private static final QueueElement POISONED = new QueueElement(null);
    }

    public AsyncCopyItemWriter(DataSource defaultDataSource,
                               @WriterType.CopyAsyncWriterQualifier AsyncTaskExecutor writerTaskExecutor,
                               BatchLoadJobParameters jobParameters) {
        this.queue = new LinkedBlockingQueue<>();
        this.taskExecutor = writerTaskExecutor;
        String tableName = jobParameters.getTestTable().getTableName();
        this.backgroundTask = new FutureTask<>(() -> {
            try (var outputStream = new ItemPGOutputStream<>(defaultDataSource, tableName)) {
                for (QueueElement element = null;
                     element != QueueElement.POISONED;
                     element = queue.poll(100, TimeUnit.MILLISECONDS)) {
                    if (element == null) continue;
                    for (InputFileItem item : element.chunk) {
                        outputStream.write(item);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void write(Chunk<? extends InputFileItem> chunk) {
        queue.add(new QueueElement(chunk));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        taskExecutor.execute(backgroundTask);
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            queue.add(QueueElement.POISONED);
            backgroundTask.get();
        } catch (Exception e) {
            throw new ItemStreamException("Unable to close " + this.getClass().getName(), e);
        }
    }
}
