package pete.eremeykin.bulkinsert.input.generator;

import net.datafaker.Faker;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import pete.eremeykin.bulkinsert.input.InputFileItem;

import java.util.List;
import java.util.stream.Collectors;

@Component
class RandomInputFileItemsGenerator implements ItemsGenerator {
    private final Faker faker = new Faker();

    @Override
    public InputFileItem generateNext() {
        String songName = capitalized(List.of(
                faker.verb().ingForm(), faker.dog().name()
        ));
        String artist = faker.kpop().girlGroups();
        String albumName = capitalized(List.of(
                faker.size().adjective(), faker.animal().name()
        ));
        return new InputFileItem(
                songName,
                artist,
                albumName
        );
    }

    private String capitalized(List<String> parts) {
        return parts.stream()
                .map(StringUtils::capitalize)
                .collect(Collectors.joining(" "));
    }
}
