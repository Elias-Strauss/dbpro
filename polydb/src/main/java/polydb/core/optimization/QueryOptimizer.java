package polydb.core.optimization;

public interface QueryOptimizer<Input, Output> {

    Output optimizePlan(Input validatedPlan);
}
