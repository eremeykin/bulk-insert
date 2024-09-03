package pete.eremeykin.bulkinsert.load.batch;

import org.springframework.beans.factory.annotation.Qualifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

enum ReaderType {
    REAL,
    FAKE,
    ;

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE, ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Qualifier("realReaderQualifier")
    @interface RealReaderQualifier {
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target({ElementType.TYPE, ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
    @Qualifier("fakeReaderQualifier")
    @interface FakeReaderQualifier {
    }
}
