package dataparser;

import com.google.common.base.Stopwatch;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

public class CreateGraph {
    public static void main (String[] args) throws IOException{
        Stopwatch timer = new Stopwatch().start();
        //ImportToOracleNoSQL imp = new ImportToOracleNoSQL();
        //Connect connect = new Connect();
        
        //imp.importVertices();
        //imp.importEdges("/user/oracle/pgx/myout/part-m-00000");
        //imp.writeEdges();
        
        //test.importVertices();
        //test.importEdges("/user/oracle/pgx/myout1/part-m-00000");
        //test.updateEdges("/user/oracle/pgx/myout1/part-m-00000");
        //test.updEdges();
        //test.createHdfsFile();
        //test.getEdge();
        //Connection con = connect.connectHive();
        
        //HiveQueries qHive = new HiveQueries(con);
        //qHive.createTagCountTable();
        
        timer.stop();
        
        System.out.println("Time: " + timer.elapsedTime(TimeUnit.SECONDS) + " sec");
        System.out.println("Time: " + timer.elapsedTime(TimeUnit.MINUTES) + " min");
        System.out.println("Time: " + timer.elapsedTime(TimeUnit.HOURS) + " hours");
        
        /*
        PgNosqlGraphConfig cfg = GraphConfigBuilder.forPropertyGraphNosql()
                                                   .setName("my_graph")
                                                   .setHosts(Arrays.asList(hhosts))
                                                   .setStoreName(StoreConfig.KVSTORE_NAME)
                                                   .build();
        */
    }
}
