package polydb.core.translation.Functions;

import org.qcri.rheem.core.function.FunctionDescriptor;

public class SumFunction implements FunctionDescriptor.SerializableBinaryOperator<Record> {

    final int aggIndex;

    public SumFunction(int aggIndex) {
        this.aggIndex = aggIndex;
    }

    @Override
    public Record apply(Record r1, Record r2) {
        int sum = Integer.parseInt(r1.getString(aggIndex)) + Integer.parseInt(r2.getString(aggIndex));
        Object[] values = r1.getValues();
        values[aggIndex] = Integer.toString(sum);
        return new Record(values);
    }
}
