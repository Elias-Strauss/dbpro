package polydb.pipelines;

public class ScalarUdf {

    private String name;
    private Class<?> clazz;
    private Class<?>[] argTypes;

    public ScalarUdf(String name, Class<?> clazz, Class<?>[] argTypes) {
        this.name = name;
        this.clazz = clazz;
        this.argTypes = argTypes;
    }

    public String getName() {
        return name;
    }

    public Class<?> getClazz() {
        return clazz;
    }

    public Class<?>[] getArgTypes() {
        return argTypes;
    }
}
