# mt_csv_distinct_network
Stored Procedure to output the distinct network of all users

Stored Procedure
------------

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/stored_procedures-1.0-SNAPSHOT.jar`,
that can be copied to the `plugin` directory of your Neo4j instance.

    cp target/stored_procedures-1.0-SNAPSHOT.jar neo4j-enterprise-3.3.4/plugins/.


Edit your Neo4j/conf/neo4j.conf file by adding this line:

    dbms.security.procedures.unrestricted=com.maxdemarzi.*

Run the procedure:

    CALL com.maxdemarzi.distinct_network_to_file_mt(max_threads, file);
    
    CALL com.maxdemarzi.distinct_network_to_file_mt(8, 'myoutput.csv')