package optimizers.calcite;

import org.apache.calcite.schema.Statistic;
import org.checkerframework.checker.nullness.qual.Nullable;

public class CsvTableStatistic implements Statistic {

    private final long rowCount;

    public CsvTableStatistic(long rowCount) {
        this.rowCount = rowCount;
    }

    @Override
    public @Nullable Double getRowCount() {
        return (double) rowCount;
    }
}
