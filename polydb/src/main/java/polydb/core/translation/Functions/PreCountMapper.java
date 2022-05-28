package polydb.core.translation.Functions;

import org.qcri.rheem.core.function.FunctionDescriptor;

public class PreCountMapper implements FunctionDescriptor.SerializableFunction<Record, Record> {

    final int index;

    public PreCountMapper(int index) {
        this.index = index;
    }

    @Override
    public Record apply(Record record) {
        Object[] values = record.getValues();
        if (index == -1){
            String[] nv = new String[values.length+1];
            for (int i = 0; i < values.length; i++) {
                nv[i] = (String) values[i];
            }
            nv[values.length] = "1";
            Record out = new Record(nv);
            //System.out.println(out);
            return out;
        }else {
            values[index] = values[index] == null ? "0" : "1";
            //System.out.println(Arrays.toString(values));
            return new Record(values);
        }
    }
}
