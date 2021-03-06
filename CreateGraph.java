package dataparser;

import com.google.common.base.Stopwatch;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

public class CreateGraph {
    public static void main (String[] args){
        Stopwatch timer = new Stopwatch().start();
        
        // Comment out the next three lines when running the update lines commented below
        ImportToOracleNoSQL imp = new ImportToOracleNoSQL();        
        imp.importVertices();
        imp.importEdges("/user/oracle/pgx/myout/part-m-00000");
        
        // Uncomment only for the second part. The initial graph data MUST be created.
        //Update update = new Update();
        //update.updateOracleNoSQLGraph("/user/oracle/pgx/newTags.txt");
        
        timer.stop();
        
        System.out.println("Time: " + timer.elapsedTime(TimeUnit.MINUTES) + " min");
    }
}
