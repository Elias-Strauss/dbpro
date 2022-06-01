package polydb.core.queryinterface;

public interface QueryInterface<Intermediate, Output> {

    Intermediate getLogicalPlan(String query);

    Output validateLogicalPlan(Intermediate logicalPlan);
}
