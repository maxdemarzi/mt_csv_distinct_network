package com.maxdemarzi;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.maxdemarzi.results.StringResult;
import com.maxdemarzi.schema.Labels;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.*;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class Procedures {

    public static final LoadingCache<Integer, String> names = Caffeine.newBuilder()
            .maximumSize(26_000_000)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build(Procedures::getName);

    private static String getName(Integer nodeId) {
        return (String)dbAPI.getNodeById((long)nodeId).getProperty("name", "NOTAPERSON");
    }

    public static final LoadingCache<Integer, RoaringBitmap> related = Caffeine.newBuilder()
            .maximumSize(26_000_000)
            .expireAfterWrite(1, TimeUnit.HOURS)
            .build(Procedures::getRelationships);

    private static RoaringBitmap getRelationships(Integer nodeId) {
        RoaringBitmap related = new RoaringBitmap();
        for (Relationship r : dbAPI.getNodeById((long)nodeId).getRelationships()) {
            related.add((int)(long) r.getOtherNodeId(nodeId));
        }
        return related;
    }

    public static RoaringBitmap unwanted = new RoaringBitmap();

    @Context
    public GraphDatabaseAPI db;

    @Context
    public Log log;

    private static GraphDatabaseAPI dbAPI;

    @Procedure(name = "com.maxdemarzi.distinct_network_to_file_mt", mode = Mode.WRITE)
    @Description("CALL com.maxdemarzi.distinct_network_to_file_mt(threads, file)")
    public Stream<StringResult> DistinctNetworkToFileMultiThreaded(@Name("number") Number number, @Name("file") String file) throws InterruptedException {
        dbAPI = db;
        int threads = Math.min(Runtime.getRuntime().availableProcessors(), number.intValue());
        final ExecutorService service = Executors.newFixedThreadPool(threads);

        // Get all Locations and Organizations into Unwanted.

        Iterator<Node> it = db.findNodes(Labels.Location).stream().iterator();
        while (it.hasNext()) {
            unwanted.add((int)(long)it.next().getId());
        }

        it = db.findNodes(Labels.Organization).stream().iterator();
        while (it.hasNext()) {
            unwanted.add((int)(long)it.next().getId());
        }

        ArrayList<Long> people = new ArrayList<>();
        it = db.findNodes(Labels.Person).stream().iterator();
        while (it.hasNext()) {
            people.add(it.next().getId());
        }

        for (int i = 0; i <= threads; i++) {
            service.execute(new Work(db, people, i, threads, file));
        }
        try {
            service.shutdown();
            service.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("tasks interrupted");
        } finally {
            if (!service.isTerminated()) {
                System.err.println("cancel tasks");
            }
            service.shutdownNow();
            System.out.println("shutdown finished");
        }

        return Stream.of(new StringResult("Results written to: " + file + "1-" + threads));
    }

}
