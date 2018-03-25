package com.maxdemarzi;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;

public class DistinctNetworkToFileMultiThreadedTest {

    @Rule
    public final Neo4jRule neo4j = new Neo4jRule()
            .withFixture(MODEL_STATEMENT)
            .withProcedure(Procedures.class);

    @Test
    public void shouldGetDistinctNetwork() {
        HTTP.Response response = HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), QUERY);
        String results = getResultRow(response);
        assertTrue(results.startsWith("Results written to"));
    }

    private static final HashMap<String, Object> QUERY = new HashMap<String, Object>(){{
        put("statements", new ArrayList<Map<String, Object>>() {{
            add(new HashMap<String, Object>() {{
                put("statement", "CALL com.maxdemarzi.distinct_network_to_file_mt(2,'paths.csv') YIELD value RETURN value");
            }});
        }});
    }};
    private static final String MODEL_STATEMENT =
            "CREATE (c1:Person {name:'c1'})" +
            "CREATE (c2:Person {name:'c2'})" +
            "CREATE (c3:Person {name:'c3'})" +
            "CREATE (c4:Person {name:'c4'})" +
            "CREATE (c5:Person {name:'c5'})" +
            "CREATE (c6:Organization {name:'c6'})" +
            "CREATE (c7:Person {name:'c7'})" +
            "CREATE (c8:Location {name:'c8'})" +
            "CREATE (c9:Person {name:'c9'})" +
            "CREATE (c10:Person {name:'c10'})" +
            "CREATE (c11:Person {name:'c11'})" +
            "CREATE (c1)-[:KNOWS]->(c2)" +
            "CREATE (c2)-[:KNOWS]->(c3)" +
            "CREATE (c3)-[:KNOWS]->(c4)" +
            "CREATE (c4)-[:KNOWS]->(c1)" +
            "CREATE (c1)-[:KNOWS]->(c5)" +
            "CREATE (c5)-[:KNOWS]->(c3)" +
            "CREATE (c5)-[:IS_IN]->(c6)" +
            "CREATE (c6)<-[:IS_IN]-(c7)" +
            "CREATE (c7)-[:LIVES_AT]->(c8)" +
            "CREATE (c8)<-[:LIVES_AT]-(c9)" +
            "CREATE (c9)-[:KNOWS]->(c10)" +
            "CREATE (c11)-[:KNOWS]->(c10)" ;


    static String  getResultRow(HTTP.Response response) {
        Map actual = response.content();
        ArrayList<HashMap<String, Object>> results = (ArrayList)actual.get("results");
        ArrayList<Map<String, ArrayList>> data = (ArrayList)results.get(0).get("data");
        String result = (String)data.get(0).get("row").get(0);

        return result;
    }
}
