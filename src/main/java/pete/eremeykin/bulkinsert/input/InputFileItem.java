package pete.eremeykin.bulkinsert.input;

import lombok.*;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class InputFileItem {
    private String name;
    private String artist;
    private String albumName;
}
