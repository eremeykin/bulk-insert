package pete.eremeykin.bulkinsert.load.batch;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.stereotype.Component;
import pete.eremeykin.bulkinsert.input.OutputItem;
import pete.eremeykin.bulkinsert.load.batch.CopyUtils.LinePGOutputStream;

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
class AsyncCopyItemWriter implements ItemStreamWriter<OutputItem> {
    private final BlockingQueue<QueueElement<Chunk<? extends OutputItem>>> queue;
    private final FutureTask<Void> backgroundTask;
    private final AsyncTaskExecutor taskExecutor;

    private record QueueElement<T>(T value) {
        private static final QueueElement<?> POISONED = new QueueElement<>(null);

        @SuppressWarnings("unchecked")
        // always succeeds as null can be cast to any type
        private static <T> QueueElement<T> getPoisoned() {
            return (QueueElement<T>) POISONED;
        }
    }

    public AsyncCopyItemWriter(DataSource defaultDataSource,
                               @WriterType.CopyAsyncWriterQualifier AsyncTaskExecutor writerTaskExecutor,
                               BatchLoadJobParameters jobParameters) {
        this.queue = new LinkedBlockingQueue<>();
        this.taskExecutor = writerTaskExecutor;
        String tableName = jobParameters.getTestTable().getTableName();
        this.backgroundTask = new FutureTask<>(() -> {
            try (var outputStream = new LinePGOutputStream(defaultDataSource, tableName)) {
                for (QueueElement<Chunk<? extends OutputItem>> element = null;
                     element != QueueElement.POISONED;
                     element = queue.poll(100, TimeUnit.MILLISECONDS)) {
                    if (element == null) continue;
                    for (OutputItem item : element.value) {
                        CopyUtils.writeItem(item, outputStream);
                    }
                }
                return null;
            }
        });
    }

    @Override
    public void write(Chunk<? extends OutputItem> chunk) {
        queue.add(new QueueElement(chunk));
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        taskExecutor.execute(backgroundTask);
    }

    @Override
    public void close() throws ItemStreamException {
        try {
            queue.add(QueueElement.getPoisoned());
            backgroundTask.get();
        } catch (Exception e) {
            throw new ItemStreamException("Unable to close " + this.getClass().getName(), e);
        }
    }
}
