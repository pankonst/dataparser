package dataparser;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import java.sql.Connection;

import java.util.Arrays;

import java.util.List;

import oracle.kv.KVStore;
import oracle.kv.KVStoreConfig;
import oracle.kv.KVStoreFactory;

import oracle.kv.StatementResult;
import oracle.kv.table.TableAPI;

import oracle.pg.nosql.OraclePropertyGraph;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Update {
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
    
    public Update() {
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
    
    public void updateEdges(String hdfsPath){
        String fileContent = null;
        InputStream fileContentStream = null;
        String[] lineParts;
        String[] tagsArray;
        
        System.out.println("Connecting to HDFS...");
        
        Connect connectHdfs = new Connect();
        fs = connectHdfs.connectHadoop();
        
        System.out.println("Connection to HDFS established...\n");
        
        Path path = new Path(hdfsPath);
        
        if(fs!=null){            
            // Convert out fileContent string to an InputStream
            try{
                fileContent = IOUtils.toString(fs.open(path), "UTF-8");
                
                //System.out.println("File contains the following: \n" + fileContent);
                
                fileContentStream = IOUtils.toInputStream(fileContent, "UTF-8");
                BufferedReader br = new BufferedReader(new InputStreamReader(fileContentStream));
                String line = br.readLine();
                
                BufferedWriter bw =  new BufferedWriter(new OutputStreamWriter(fs.create(stagingPairsPath, true), "UTF-8"));

                while (line!=null){                  
                    lineParts = line.split("\\|");
                    //System.out.println(lineParts[0] + " : " + lineParts[1]);
                    
                    String tagsStage = lineParts[1].substring(1, lineParts[1].length() - 1);
                    //System.out.println(tagsStage);
                    
                    tagsArray = tagsStage.split("><");
                    
                    createHdfsStageFile(tagsArray[0], Arrays.copyOfRange(tagsArray, tagsArray.length-(tagsArray.length-1), tagsArray.length), bw);
                                      
                    line = br.readLine();                   
                }
                
                bw.close();
                br.close();
                
                Connection hiveConn = connect.connectHive();
                HiveQueries qHive = new HiveQueries(hiveConn);                
                // Create tag pairs table to aggregate on
                qHive.createTagPairTable();
                // Create an HDFS file(table) with the aggregated tag pairs. The third column is the total number they appear in the staging file
                qHive.createTagCountTable();
                
                // Write the edges to NoSQL
                updateEdges();
                
                fs.close();
                count = 0L;
                // Drop the staging tables crated earlier
                qHive.dropStageTables();
                
                // Reinitialize the counter
                //this.count = BigInteger.valueOf(0);
            } catch(IOException e){
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
            
            opg.commit();
            opg.closeKVStore();
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

    private void updateEdges() throws IOException{
        Edge e;
        String[] lineParts;
        String[] tags;
        String firstTag;
        String secondTag;
        int weight;
        Vertex v1 = null;
        Vertex v2 = null;
        
        String fileContent = IOUtils.toString(fs.open(stagingAggPairsPath), "UTF-8");
        InputStream fileContentStream = IOUtils.toInputStream(fileContent, "UTF-8");
        BufferedReader br = new BufferedReader(new InputStreamReader(fileContentStream));
        
        String line = br.readLine();
        
        while(line!=null){
            count++;
            
            lineParts = line.split("\\|");
            tags = lineParts[0].split("\\s");
            
            firstTag = tags[0];
            secondTag = tags[1];
            weight = Integer.valueOf(lineParts[1]);
            
            v1 = opg.getVertex(firstTag);
            v2 = opg.getVertex(secondTag);
            
            if(v1 == null || v2 == null){
                line = br.readLine();
                continue;
            }
            
            StatementResult sr = kvstore.executeSync("select svid, dvid from graphStage where svid=\"" + v1 + "\" and dvid=\"" + v2 + "\"");
            
            List<Long> srList = IteratorUtils.toList(sr.iterator());
            long eid = srList.get(0);
            
            e = opg.getEdge(eid);
            
            if(srList.size() != 0){
                if((Integer)e.getProperty("weight") == weight){
                    line = br.readLine();
                    continue;
                } else {
                    e.setProperty("weight", weight);
                }                
            } else {
                e = opg.addEdge((long)count, v1, v2, "connects");
                e.setProperty("weight", weight);
            }
        }
        
        br.close();
    }
}