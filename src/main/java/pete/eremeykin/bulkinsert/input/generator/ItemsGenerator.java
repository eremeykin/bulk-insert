package pete.eremeykin.bulkinsert.input.generator;

import pete.eremeykin.bulkinsert.input.InputItem;

interface ItemsGenerator {

    InputItem generateNext();

}
