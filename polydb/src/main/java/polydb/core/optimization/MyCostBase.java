package polydb.core.optimization;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptUtil;

public class MyCostBase implements RelOptCost {

    public double rowCount;
    public double cpu;
    public double io;

    public MyCostBase() {
        this.rowCount = 0D;
        this.cpu = 0D;
        this.io = 0D;
    }

    public MyCostBase(double rowCount, double cpu, double io) {
        this.rowCount = rowCount;
        this.cpu = cpu;
        this.io = io;
    }


    static final MyCostBase INFINITY = new MyCostBase(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        @Override
        public String toString() {
            return "{huge}";
        }};

    static final MyCostBase HUGE = new MyCostBase(Double.MAX_VALUE, Double.MAX_VALUE, Double.MAX_VALUE) {
        @Override
        public String toString() {
            return "{huge}";
        }};

    static final MyCostBase ZERO = new MyCostBase(0.0, 0.0, 0.0) {
        @Override
        public String toString() {
            return "{0}";
        }
    };

    static final MyCostBase TINY = new MyCostBase(1.0, 1.0, 0.0) {
        @Override
        public String toString() {
            return "{tiny}";
        }
    };


    // The Cost Factory!  ===============================================

    public static class MyCostFactory implements RelOptCostFactory {
        public RelOptCost makeCost() {
            return new MyCostBase(0, 0, 0);
        }

        public RelOptCost makeCost(double dRows, double dCpu, double dIo) {
            return new MyCostBase(dRows, dCpu, dIo);
        }

        public RelOptCost makeInfiniteCost() {
            return MyCostBase.INFINITY;
        }

        public RelOptCost makeHugeCost() {
            return MyCostBase.HUGE;
        }

        public RelOptCost makeTinyCost() {
            return MyCostBase.TINY;
        }

        public RelOptCost makeZeroCost() {
            return MyCostBase.ZERO;
        }
    }

    // ==================================================================

    @Override
    public double getRows() {
        return 0;
    }

    @Override
    public double getCpu() {
        return 0;
    }

    @Override
    public double getIo() {
        return 0;
    }

    @Override
    public boolean isInfinite() {
        return  (this == INFINITY)
                || (this.cpu == Double.POSITIVE_INFINITY)
                || (this.io == Double.POSITIVE_INFINITY)
                || (this.rowCount == Double.POSITIVE_INFINITY);
    }

    @Override
    public boolean equals(RelOptCost relOptCost) {
        return false;
    }

    @Override
    public boolean isEqWithEpsilon(RelOptCost other) {
        if (!(other instanceof MyCostBase)) { return false; }

        MyCostBase that = (MyCostBase) other;
        return (this == that) ||
                ((Math.abs(this.cpu - that.cpu) < RelOptUtil.EPSILON)
                        && (Math.abs(this.io - that.io) < RelOptUtil.EPSILON)
                        && (Math.abs(this.rowCount - that.rowCount) < RelOptUtil.EPSILON));
    }

    @Override
    public boolean isLe(RelOptCost other) {
        return isLt(other) || equals(other);
    }

    @Override
    public boolean isLt(RelOptCost other) {
        MyCostBase that = (MyCostBase) other;
        return this.rowCount < that.rowCount;
    }

    @Override
    public RelOptCost plus(RelOptCost other) {
        MyCostBase that = (MyCostBase) other;
        if ((this == INFINITY) || (that == INFINITY)) { return INFINITY; }
        return new MyCostBase(this.rowCount + that.rowCount, this.cpu + that.cpu, this.io + that.io);
    }

    @Override
    public RelOptCost minus(RelOptCost other) {
        if (this == INFINITY) { return this; }
        MyCostBase that = (MyCostBase) other;
        return new MyCostBase(this.rowCount - that.rowCount, this.cpu - that.cpu, this.io - that.io);
    }

    @Override
    public RelOptCost multiplyBy(double factor) {
        if (this == INFINITY) { return this; }
        return new MyCostBase(rowCount * factor, cpu * factor, io * factor);
    }

    // Compute the geometric average of the ratios of all of the factors
    // which are non-zero and finite.
    @Override
    public double divideBy(RelOptCost cost) {
        MyCostBase that = (MyCostBase) cost;
        double d = 1;
        double n = 0;
        if ((this.rowCount != 0)
                && !Double.isInfinite(this.rowCount)
                && (that.rowCount != 0)
                && !Double.isInfinite(that.rowCount)) {
            d *= this.rowCount / that.rowCount;
            ++n;
        }
        if ((this.cpu != 0)
                && !Double.isInfinite(this.cpu)
                && (that.cpu != 0)
                && !Double.isInfinite(that.cpu)) {
            d *= this.cpu / that.cpu;
            ++n;
        }
        if ((this.io != 0)
                && !Double.isInfinite(this.io)
                && (that.io != 0)
                && !Double.isInfinite(that.io)) {
            d *= this.io / that.io;
            ++n;
        }

        if (n == 0) { return 1.0; }
        return Math.pow(d, 1 / n);
    }

    @Override
    public String toString() {
        return "{" + rowCount + " rows, " + cpu + " cpu, " + io + " io}";
    }

}