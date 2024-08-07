package pete.eremeykin.bulkinsert.input;

import lombok.*;

import java.util.UUID;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class OutputItem {
    private UUID id;
    private String name;
    private String artist;
    private String albumName;
}
