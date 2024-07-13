package pete.eremeykin.bulkinsert.input.generator;

import pete.eremeykin.bulkinsert.input.InputFileItem;

interface ItemsGenerator {

    InputFileItem generateNext();

}
