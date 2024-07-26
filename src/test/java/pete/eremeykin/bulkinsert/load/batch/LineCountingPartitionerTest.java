package pete.eremeykin.bulkinsert.load.batch;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.item.ExecutionContext;

import java.util.Map;
import java.util.stream.Stream;

@ExtendWith(MockitoExtension.class)
class LineCountingPartitionerTest {
    private LineCountingPartitioner lineCountingPartitioner;
    @Mock
    private LineCountingService lineCountingService;

    public static Stream<Arguments> arguments() {
        return Stream.of(
                Arguments.of(7)
        );
    }

    @BeforeEach
    void setUp() {
        lineCountingPartitioner = new LineCountingPartitioner(lineCountingService);
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void testPartitioner(int gridSize) {
        Mockito.when(lineCountingService.countLines()).thenReturn(55);
        Map<String, ExecutionContext> partitionMap = lineCountingPartitioner.partition(gridSize);
        System.out.println(partitionMap);
    }
}