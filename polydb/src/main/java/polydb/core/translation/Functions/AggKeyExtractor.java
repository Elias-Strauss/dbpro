package polydb.core.translation.Functions;

import org.apache.calcite.util.ImmutableBitSet;
import org.qcri.rheem.core.function.FunctionDescriptor;

public class AggKeyExtractor implements FunctionDescriptor.SerializableFunction<Record, String> {

    final ImmutableBitSet groupSet;

    public AggKeyExtractor(ImmutableBitSet groupSet) {
        this.groupSet = groupSet;
    }

    @Override
    public String apply(Record record) {
        String result = "";
        int i = groupSet.nextSetBit(0);
        while (i != -1) {

            result += record.getString(i);
            i = groupSet.nextSetBit(i + 1);
        }
        return result;

    }
}
