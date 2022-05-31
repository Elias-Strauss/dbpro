package polydb.global;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;


/**
 * Factory that creates a {@link GlobalSchema}.
 */
public class GlobalSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus parentSchema, String name, Map<String, Object> operand) {
        return new GlobalSchema(operand.get("system").toString(), operand.get("database").toString());
    }
}
