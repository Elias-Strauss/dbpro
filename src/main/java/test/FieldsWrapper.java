package test;

import java.io.Serializable;
import java.util.ArrayList;

public class FieldsWrapper implements Serializable {
    public FieldsWrapper(ArrayList<String> inputfields) {
        this.fields = inputfields;
    }

    public ArrayList<String> fields;

}
