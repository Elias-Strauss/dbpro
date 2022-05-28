package polydb.core.translation.Functions;

import org.qcri.rheem.core.function.FunctionDescriptor;

public class MinFunction implements FunctionDescriptor.SerializableBinaryOperator<Record> {

    final int aggIndex;

    public MinFunction(int aggIndex) {
        this.aggIndex = aggIndex;
    }

    public Record apply(Record r1, Record r2) {
        int max = Math.min(Integer.parseInt(r1.getString(aggIndex)),Integer.parseInt(r2.getString(aggIndex)));
        Object[] values = r1.getValues();
        values[aggIndex] = Integer.toString(max);
        return new Record(values);
    }
}
