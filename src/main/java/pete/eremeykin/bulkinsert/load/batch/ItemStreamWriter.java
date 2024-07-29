package pete.eremeykin.bulkinsert.load.batch;

import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;

interface ItemStreamWriter<T> extends ItemWriter<T>, ItemStream {
}
