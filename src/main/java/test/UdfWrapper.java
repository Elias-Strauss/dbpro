package test;

import java.io.Serializable;

public class UdfWrapper implements Serializable {
    public String name;
    public Object object;
    public Class<?>[] params;

    public UdfWrapper(String name, Object object, Class<?>[] params) {
        this.name = name;
        this.object = object;
        this.params = params;
    }
}
