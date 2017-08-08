package dataparser;

import com.google.common.base.Stopwatch;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

public class CreateGraph {
    public static void main (String[] args){
        Stopwatch timer = new Stopwatch().start();
        
        //ImportToOracleNoSQL imp = new ImportToOracleNoSQL();
        
        //imp.importVertices();
        //imp.importEdges("/user/oracle/pgx/myout/part-m-00000");
        
        Update update = new Update();
        update.updateOracleNoSQLGraph("/user/oracle/pgx/newTags.txt");
        
        timer.stop();
        
        System.out.println("Time: " + timer.elapsedTime(TimeUnit.MINUTES) + " min");
    }
}
