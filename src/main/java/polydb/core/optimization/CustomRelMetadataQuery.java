package polydb.core.optimization;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;


public class CustomRelMetadataQuery extends RelMetadataQuery {

    @Override
    public Double getRowCount(RelNode rel) {

        System.out.println("In Method: mq::getRowCount()");

        if (rel.getTable() != null){

            final List<String> qualifiedName = rel.getTable().getQualifiedName();
            final String system = qualifiedName.get(0);
            final String table = qualifiedName.get(1);
            //TODO: Find a way to extract database name (maybe from metaRepo)

            System.out.println("table: "+table);

            if (table.equals("clicks"))
                return 10d;
            if (table.equals("photos"))
                return 10d;
            if (table.equals("users"))
                return 100000d;
        }
        return super.getRowCount(rel);
    }

    @Override
    public Double getAverageRowSize(RelNode rel) {
        System.out.println("Got into getAverageRowSize!");
        return 99999999D;
    }
}
