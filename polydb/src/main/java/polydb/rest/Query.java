package polydb.rest;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Query {
    private String query;

    public String getQuery() {
        return this.query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
