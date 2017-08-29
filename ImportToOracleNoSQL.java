package dataparser;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import oracle.pg.nosql.OraclePropertyGraph;

import org.apache.hadoop.fs.FileSystem;
import org.apache.commons.io.*;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.io.InputStreamReader;

import java.io.OutputStreamWriter;
import java.util.Arrays;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

import oracle.kv.table.TableAPI;

public class ImportToOracleNoSQL {
    private final Path stagingPairsPath = new Path(Config.STAGING_PAIRS_PATH);
    private final Path stagingAggPairsPath = new Path(Config.STAGING_AGG_PAIRS_PATH);

    private OraclePropertyGraph opg;
    private String[] hhosts = { Config.KVSTORE_URL };
    private KVStoreConfig kvconfig;
    private KVStore kvstore;
    private Connect connect;
    private Long count = 0L;
    private TableAPI tableApi;
    private FileSystem fs;

    public ImportToOracleNoSQL() {
        super();
        this.connect = new Connect();
        this.kvconfig = connect.getNoSQLConfig(hhosts, Config.KVSTORE_NAME);
        this.kvstore = KVStoreFactory.getStore(kvconfig);
        this.tableApi = kvstore.getTableAPI();
        
        System.out.println("Retreiving NoSQL Property Graph...");
        
        try{
            opg = OraclePropertyGraph.getInstance(kvconfig, Config.KVGRAPH_NAME);
        } catch(Exception e){
            System.out.println(e.getMessage());
        }
        
        System.out.println("NoSQL Property Graph retreived...\n");
    }
    
    public void importVertices(){
        //KVStore kvstore = KVStoreFactory.getStore(kvconfig);
        
        System.out.println("Connecting to Oracle Database...");
        
        // Get Oracle Database connection
        Connection dbConn = connect.connectRDB(Config.JDBC_RDBMS_URL, "pgx", "pgx");
        
        if(dbConn != null && opg != null){
            System.out.println("Connection to Database established...");
            
            // Query the database
            try{
                Statement state = dbConn.createStatement();
                ResultSet resSet = state.executeQuery("SELECT * FROM PGX.TAGS");
                int i = 1;
                
                while(resSet.next()){
                    //String id = resSet.getString("ID");
                    String tagName = resSet.getString("TAGNAME");
                    int count = Integer.valueOf(resSet.getString("COUNT"));
                    
                    // Import to NoSQL vertices
                    Vertex v;
                    v = opg.addVertex(tagName);
                    v.setProperty("tagName", tagName);
                    v.setProperty("count",  count);
                    i++;
                }
                
                dbConn.commit();
                dbConn.close();
            } catch(SQLException e){
                System.out.println(e.getMessage());
            }
            
            // Commit changes to NoSQL
            opg.commit();
            System.out.println("Vertices imported successfully...");
        }
    }
    
    public void importEdges(String hdfsPath){        
        String fileContent = null;
        InputStream fileContentStream = null;
        String[] lineParts;
        String[] tagsArray;
        
        System.out.println("Connecting to HDFS...");
        
        Connect connectHdfs = new Connect();
        fs = connectHdfs.connectHadoop();
        
        System.out.println("Connection to HDFS established...\n");
        
        Path path = new Path(hdfsPath);

	// Get the content of the file
        if(fs!=null){            
            // Convert out fileContent string to an InputStream
            try{
                fileContent = IOUtils.toString(fs.open(path), "UTF-8");
                
                fileContentStream = IOUtils.toInputStream(fileContent, "UTF-8");
                BufferedReader br = new BufferedReader(new InputStreamReader(fileContentStream));
                String line = br.readLine();
                
                BufferedWriter bw =  new BufferedWriter(new OutputStreamWriter(fs.create(stagingPairsPath, true), "UTF-8"));
                
                while(line!=null){                  
                    lineParts = line.split("\\|");
                    
                    String tagsStage = lineParts[1].substring(1, lineParts[1].length() - 1);
                    
                    tagsArray = tagsStage.split("><");
                    
                    // Create an HDFS file with all the tag pairs
                    createHdfsStageFile(tagsArray[0], Arrays.copyOfRange(tagsArray, tagsArray.length-(tagsArray.length-1), tagsArray.length), bw);
                    
                    line = br.readLine();                   
                }
                
                bw.close();
                br.close();
                
                System.out.println("HDFS file processing complete...");
                
                Connection hiveConn = connect.connectHive();
                HiveQueries qHive = new HiveQueries(hiveConn);                
                // Create tag pairs table to aggregate on
                qHive.createTagPairTable();
                // Create an HDFS file(table) with the aggregated tag pairs. The third column is the total number they appear in the staging file
                qHive.createTagCountTable();
                
                System.out.println("Aggregated staging table created...");
                
                // Write the edges to NoSQL
                writeEdges();
                
                fs.close();
                // Reinitialize the counter
                count = 0L;
                
                System.out.println("Edges imported successfully...");
                
                // Drop the staging tables crated earlier
                qHive.dropStageTables();
                
                System.out.println("Staging tables dropped...");
            } catch(IOException e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
            
            opg.commit();
            opg.closeKVStore();
            
            System.out.println("Process complete");
        }
    }
    
    public void createHdfsStageFile(String first, String[] rest, BufferedWriter bw){
        if(rest.length == 0){
            return;
        }
        
        try{
            for(int i=0; i<rest.length; i++){
                bw.write(first + " " + rest[i]);
                bw.newLine();
            }
        } catch(IOException e){
            System.out.println(e.getMessage());
        }
        
        createHdfsStageFile(rest[0], Arrays.copyOfRange(rest, rest.length-(rest.length-1), rest.length), bw);
        return;
    }
    
    private void writeEdges() throws IOException{
        Edge e;
        String[] lineParts;
        String[] tags;
        String firstTag;
        String secondTag;
        long weight;
        Vertex v1 = null;
        Vertex v2 = null;
        
        String fileContent = IOUtils.toString(fs.open(stagingAggPairsPath), "UTF-8");
        InputStream fileContentStream = IOUtils.toInputStream(fileContent, "UTF-8");
        BufferedReader br = new BufferedReader(new InputStreamReader(fileContentStream));
        
        String line = br.readLine();
        
        while(line!=null){
            lineParts = line.split("\\|");
            tags = lineParts[0].split("\\s");
            
            firstTag = tags[0];
            secondTag = tags[1];
            weight = Integer.valueOf(lineParts[1]);
            
            v1 = opg.getVertex(firstTag);
            v2 = opg.getVertex(secondTag);
            
            // We discovered tags in the Posts.xml not included in Tags.xml. We just skip them
            if(v1 == null || v2 == null){
                line = br.readLine();
                continue;
            }
            
            e = opg.addEdge(lineParts[0], v1, v2, "connects");
            e.setProperty("weight", weight);
            
            line = br.readLine();
        }
        
        br.close();
    }
}