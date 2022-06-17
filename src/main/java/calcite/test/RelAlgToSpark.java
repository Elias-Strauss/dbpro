package calcite.test;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.*;

public class RelAlgToSpark {

    public String translatePlan(RelNode optimizedPlan){

        new RelVisitor(){
            public RelNode go(RelNode p) {
                try {
                    visit(p, 0, null);
                } catch (Exception e) {
                    // Rewriting cannot be performed
                    System.out.println(e.getMessage());
                }
                return p;
            }
            public void visit(final RelNode node, final int ordinal, final RelNode parent) {

                System.out.println(node);

                if (node instanceof TableScan) {

                }

                if (node instanceof TableModify) {

                }

                if (node instanceof Project) {

                }

                if (node instanceof Filter) {

                }

                if (node instanceof Join) {

                }

                if (node instanceof Sort) {

                }

                if (node instanceof Aggregate) {

                }




                super.visit(node, ordinal, parent);
            }
        }.
                go(optimizedPlan);

        return null;
    }

}
