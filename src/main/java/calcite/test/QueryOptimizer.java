package calcite.test;

public interface QueryOptimizer<Input, Output> {

    Output optimizePlan(Input validatedPlan);
}
