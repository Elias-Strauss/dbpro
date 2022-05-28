package polydb.core.translation.Functions;

import org.qcri.rheem.core.function.FunctionDescriptor;

public class MaxFunction implements FunctionDescriptor.SerializableBinaryOperator<Record> {

    final int aggIndex;

    public MaxFunction(int aggIndex) {
        this.aggIndex = aggIndex;
    }

    @Override
    public Record apply(Record r1, Record r2) {
        int max = Math.max(Integer.parseInt(r1.getString(aggIndex)),Integer.parseInt(r2.getString(aggIndex)));
        Object[] values = r1.getValues();
        values[aggIndex] = Integer.toString(max);
        return new Record(values);
    }
}