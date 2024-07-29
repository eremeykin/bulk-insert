package pete.eremeykin.bulkinsert.load.batch;

import org.springframework.beans.factory.annotation.Qualifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

enum WriterType {
    COPY_ASYNC,
    COPY_SYNC,
    INSERTS_DEFAULT,
    INSERTS_ADVANCED,
    ;

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE, ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Qualifier("copyAsyncWriterQualifier")
    @interface CopyAsyncWriterQualifier {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE, ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Qualifier("copySyncWriterQualifier")
    @interface CopySyncWriterQualifier {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE, ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Qualifier("insertsWriterQualifier")
    @interface InsertsWriterQualifier {
    }
}
