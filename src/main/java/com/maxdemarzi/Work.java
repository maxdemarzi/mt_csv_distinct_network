package com.maxdemarzi;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.roaringbitmap.RoaringBitmap;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class Work implements Runnable {
    private final GraphDatabaseAPI db;
    private final ArrayList<Long> people;
    private final int portion;
    private final int threads;
    private final String file;


    public Work(GraphDatabaseAPI db, ArrayList<Long> people, int portion, int threads, String file) {
        this.db = db;
        this.people = people;
        this.portion = portion;
        this.threads = threads;
        this.file = portion + "-" + file;
    }

    @Override
    public void run() {
        // Write to CSV File
        FileWriter fileWriter = null;
        CSVPrinter csvFilePrinter = null;

        // Write it to the file system
        try (Transaction tx = db.beginTx()) {
            fileWriter =  new FileWriter(file);
            csvFilePrinter = new CSVPrinter(fileWriter, CSVFormat.DEFAULT.withRecordSeparator("\n"));
            csvFilePrinter.printRecord("p2name", "p1name");
            CSVPrinter finalCsvFilePrinter = csvFilePrinter;
            int chunk = (people.size()/threads);
            int from = portion * chunk;
            int to = Math.min((portion + 1) * chunk, people.size());
            if (from > to) {
                return;
            }
            for (Long start : people.subList(from, to)) {
                Iterator<Integer> iterator;
                Iterator<Integer> internalIterator;

                // Get my name
                String startName = Procedures.names.get((int)(long)start);

                // Initialize bitmaps for iteration
                RoaringBitmap seen = new RoaringBitmap();
                RoaringBitmap nextA = new RoaringBitmap();
                RoaringBitmap nextB = new RoaringBitmap();
                seen.add((int)(long)start);

                // Add all related to seen
                seen.or(Procedures.related.get((int)(long)start));

                // Add all related nodes to next set
                nextA.or(Procedures.related.get((int)(long)start));

                // Level 2
                iterator = nextA.iterator();
                while (iterator.hasNext()) {

                    internalIterator = Procedures.related.get(iterator.next()).iterator();
                    while (internalIterator.hasNext()) {
                        int nodeId = internalIterator.next();
                        if(seen.checkedAdd(nodeId)) {
                            nextB.add(nodeId);
                        }
                    }
                }

                nextA.clear();

                // Level 3
                iterator = nextB.iterator();
                while (iterator.hasNext()) {
                    internalIterator = Procedures.related.get(iterator.next()).iterator();
                    while (internalIterator.hasNext()) {
                        int nodeId = internalIterator.next();
                        if(seen.checkedAdd(nodeId)) {
                            nextA.add(nodeId);
                        }
                    }
                }

                // Level 4
                iterator = nextA.iterator();
                while (iterator.hasNext()) {
                    internalIterator = Procedures.related.get(iterator.next()).iterator();
                    while (internalIterator.hasNext()) {
                        int nodeId = internalIterator.next();
                        // No need to check
                        seen.add(nodeId);
                    }
                }

                // remove myself
                seen.remove((int)(long)start);

                // remove unwanted
                seen.andNot(Procedures.unwanted);

                // Print them
                iterator = seen.iterator();
                while (iterator.hasNext()) {
                    finalCsvFilePrinter.printRecord(Procedures.names.get(iterator.next()), startName);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

            try {
                assert fileWriter != null;
                fileWriter.flush();
                fileWriter.close();

                assert csvFilePrinter != null;
                csvFilePrinter.close();

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

}
