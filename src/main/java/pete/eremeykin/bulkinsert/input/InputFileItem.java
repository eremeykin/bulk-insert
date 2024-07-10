package pete.eremeykin.bulkinsert.input;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@AllArgsConstructor
public class InputFileItem {
    private String name;
    private String artist;
    private String albumName;
}
