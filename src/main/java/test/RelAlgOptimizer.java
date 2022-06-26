package test;

import org.apache.calcite.rel.RelNode;

/**
 * The RelAlgOptimizer is used to optimize SQL Queries. Given a validated relational algebra plan and a set of rules,
 * it applies these rules on the logical plan. Rewrites are done by detecting and rewriting subexpressions in the plan.
 */
public class RelAlgOptimizer implements QueryOptimizer<RelNode, RelNode> {

    public RelAlgOptimizer() {
    }


    @Override
    public RelNode optimizePlan(RelNode validatedPlan) {

//        boolean enableJoinOrder = false;
//
//        if (enableJoinOrder) {
//            //TODO: Old version please update
//            HepProgram program = HepProgram.builder()
//                    .addRuleInstance(LoptOptimizeJoinRule.INSTANCE)
//                    .addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
//                    .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
//                    .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)
//                    .build();
//
//            HepPlanner hepPlanner = new HepPlanner(program, null, false, null, RelOptCostImpl.FACTORY);
//            hepPlanner.setRoot(validatedPlan);
//            RelNode firstStep = hepPlanner.findBestExp();
//            RelTraitSet requiredOutputTraits = firstStep.getTraitSet();
//            List<RelOptMaterialization> emptyMaterializations = new ArrayList<>();
//            List<RelOptLattice> emptyLattices = new ArrayList<>();
//
//            final Program joinOrder = Programs.heuristicJoinOrder(Programs.RULE_SET, false, 0);
//
//            final RelNode optimized = joinOrder.run(
//                    hepPlanner,
//                    firstStep,
//                    requiredOutputTraits,
//                    emptyMaterializations,
//                    emptyLattices
//            );
//
//            return optimized;
//        } else {
//            HepProgram program = HepProgram.builder()
//                    .addRuleInstance(ProjectFilterTransposeRule.INSTANCE)
//                    .addRuleInstance(ProjectJoinTransposeRule.INSTANCE)
//                    .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
//                    .build();
//
//            HepPlanner hepPlanner = new HepPlanner(program);
//            hepPlanner.setRoot(validatedPlan);
//            RelNode optimized = hepPlanner.findBestExp();
//            return optimized;
//        }
        return null;
    }

}
