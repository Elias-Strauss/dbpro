package optimizers.calcite;

import org.apache.calcite.schema.Statistic;
import org.checkerframework.checker.nullness.qual.Nullable;

public class GenTableStatistic implements Statistic {

    private final long rowCount;

    public GenTableStatistic(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public @Nullable Double getRowCount() {
        return (double) rowCount;
    }
}
