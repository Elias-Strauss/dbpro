package polydb.core.translation;

public interface QueryTranslator<Input, Output> {

    Output translatePlan (Input optimizedPlan) throws Exception;
}
