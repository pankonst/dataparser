package dataparser;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveQueries {
    private Connection connect;
    private Statement state;
    
    public HiveQueries(Connection connect) {
        super();
        this.connect = connect;
        
        try{
            state = connect.createStatement();
        } catch(SQLException e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
    
    public void createTagPairTable(){
        String q = "create external table tags_pairs(pairs string) " +
                       "location '/user/oracle/pgx/stage'";
        String[] query = {q};
        
        executeQuery(query);
    }
    
    public void createTagCountTable(){
        String q = "create table aggTagCount " +
                       "row format delimited " +
                       "fields terminated by '|' " +
                       "stored as textfile " +
                       "location '/user/oracle/pgx/stageAgg' " +
                       "as " +
                            "select pairs, count(1) as count " +
                            "from tags_pairs " +
                            "group by pairs " +
                            "order by count desc";
        String[] query = {q};
        
        executeQuery(query);
    }
    
    public void dropStageTables(){
        String q1 = "drop table tags_pairs";
        String q2 = "drop table aggTagCount";
        String[] query = {q1, q2};
        
        executeQuery(query);
    }
    
    private void executeQuery(String[] query){
        try{
            for(String q : query){
                state.execute(q);
            }
        } catch(SQLException e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }
}