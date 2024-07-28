package pete.eremeykin.bulkinsert.load.batch;

import lombok.RequiredArgsConstructor;
import lombok.experimental.Delegate;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemWriter;

interface ItemStreamWriter<T> extends ItemWriter<T>, ItemStream {

    @RequiredArgsConstructor
    class DelegateItemStreamWriter<T> implements ItemStreamWriter<T> {
        @Delegate(types = ItemWriter.class)
        private final ItemWriter<T> itemWriter;

        @Override
        public void open(ExecutionContext executionContext) throws ItemStreamException {
            if (itemWriter instanceof ItemStream itemStream) {
                itemStream.open(executionContext);
            }
        }

        @Override
        public void update(ExecutionContext executionContext) throws ItemStreamException {
            if (itemWriter instanceof ItemStream itemStream) {
                itemStream.update(executionContext);
            }
        }

        @Override
        public void close() throws ItemStreamException {
            if (itemWriter instanceof ItemStream itemStream) {
                itemStream.close();
            }
        }
    }
}
