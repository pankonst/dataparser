package dataparser;

import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import java.util.Map;

import oracle.kv.KVStoreConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Connect {    
    public Connect() {
        super();
    }
    
    public KVStoreConfig getNoSQLConfig(String[] kvstore_url, String kvstore_name){
        KVStoreConfig kvconfig = new KVStoreConfig(kvstore_name, kvstore_url);        
        return kvconfig;
    }
    
    public Connection connectRDB(String dbUrl, String username, String password){
        Connection conn = null;
        
        try{
            //DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
            conn = DriverManager.getConnection(dbUrl, username, password);
        } catch(SQLException e){
            System.out.println(e.getMessage());
            System.out.println(e.getLocalizedMessage());
            for(int i=0; i<e.getStackTrace().length; i++){
                System.out.println(e.getStackTrace()[i]);
            }
        }
        return conn;
    }
    
    public FileSystem connectHadoop(){
        FileSystem fs = null;
        
        Map<String, String> env = System.getenv();
        String hadoop_conf_dir = env.get("HADOOP_CONF_DIR");

        Configuration conf = new Configuration();

        // Set explicitly the configuration xml to be used
        // These lines resolved the "File file:/bigdatalite.localdomain:8020/user/oracle/pgx/myout/000000_0 does not exist." exception
        conf.addResource(new Path(hadoop_conf_dir + "/core-site.xml"));
        conf.addResource(new Path(hadoop_conf_dir + "/hdfs-site.xml"));
        
        // Set explicitly the classes to be used for local and hdfs filesystem
        /*
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        */
        
        try{
            fs = FileSystem.get(conf);
        } catch(IOException e){
            System.out.println(e.getMessage());
        }
        
        return fs;
    }
    
    public Connection connectHive(){
        Connection connect = null;
        
        try{
            Class.forName(Config.HIVE_DRIVER);
            
            // We need to connect as a existing user because connecting as an anonymous one will not allow us to run MapReduce tasks
            connect = DriverManager.getConnection(Config.HIVE_SERVER_URL, "oracle", "welcome1");
        } catch(ClassNotFoundException e){ 
            System.out.println(e.getMessage());
            e.printStackTrace();
        } catch(SQLException e){
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        
        return connect;
    }
}