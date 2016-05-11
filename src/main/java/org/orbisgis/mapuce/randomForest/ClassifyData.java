package org.orbisgis.mapuce.randomForest;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Set;
import org.h2gis.h2spatial.ut.SpatialH2UT;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;
import weka.experiment.InstanceQuery;

/**
 * Class that classifies data following a SQL request
 * @author Melvin Le Gall
 */
public class ClassifyData {

    //the model used to classifies
    private RandomForest model;
    
    //Attribute who know class
    private Attribute attClass;
    
    //Instances which is classified
    private Instances dataToClassify;
    
    //use to store index of the primary key
    private int colPk;

    //Connection at DB
    private static Connection connection;
    
    //Statement for execute query
    private static Statement stat;
    
    //temporary DB for the Test
    private static final String DB_NAME = "RandomForestTest";
    
    //name of the table where the results are stored
    private static String nameTableResult;
    
    //used to optimize performance
    private static final int BATCH_MAX_SIZE = 100;
    

    /**
     * Initialize the Classifier RandomForest with a .model file
     * @param pathModel where the .model file is store
     * @throws Exception 
     */
    public ClassifyData(String pathModel) throws Exception{
        
        Object[] obj = weka.core.SerializationHelper.readAll(pathModel);
        attClass = (Attribute) obj[0];
        model = (RandomForest) obj[1];
        
        connection = SpatialH2UT.openSpatialDataBase(DB_NAME);
        if(connection == null){
           connection = SpatialH2UT.createSpatialDataBase(DB_NAME);
        }
        stat = connection.createStatement();
    }
    
    /**
     * Initialize the Classifier RandomForest with a RandomForest.model file
     * A model for Building (75 features)
     * @param pathModel where the .model file is store
     * @throws Exception 
     */
    public ClassifyData() throws Exception{
        
        Object[] obj = weka.core.SerializationHelper.readAll(this.getClass().getResource("RandomForest.model").getPath());
        
        attClass = (Attribute) obj[0];
        model = (RandomForest) obj[1];
        
        connection = SpatialH2UT.openSpatialDataBase(DB_NAME);
        if(connection == null){
           connection = SpatialH2UT.createSpatialDataBase(DB_NAME);
        }
        stat = connection.createStatement();
    }
    
    /**
     * Transform ResultSet into a usable format for Weka (Instances)
     * Work for simple data type
     * @param res ResultSet got after an SQL request
     * @throws Exception 
     */
    public Instances resultSetToInstances(ResultSet rs) throws Exception{

        Instances inst = InstanceQuery.retrieveInstances(new InstanceQuery(),rs);
        
        inst.insertAttributeAt(attClass, inst.numAttributes());
        //inst.replaceAttributeAt(attClass, inst.numAttributes()-1);
        inst.setRelationName("classification");
        
        BufferedWriter writer = new BufferedWriter(new FileWriter(this.getClass().getResource("ResultSet.arff").getPath()));
        writer.write(inst.toString());
        writer.flush();
        writer.close();
                
        ConverterUtils.DataSource sourceTestValue = new ConverterUtils.DataSource(this.getClass().getResource("ResultSet.arff").getPath());
    	Instances value = sourceTestValue.getDataSet();
    	value.setClassIndex(value.numAttributes()-1);
        
        this.dataToClassify = value;
        return inst;
    }
    
  
    /**
     * Execute request via a connection to the database 
     * @param query query like "SELECT * FROM the_table"
     * @return ResultSet of the request
     */
    public ResultSet executeQuery(String query,String nameColPk) throws SQLException{
        
        ResultSet res = stat.executeQuery(query);
        
        this.colPk = res.findColumn(nameColPk)-1;
        return res;
    }
       
    /**
     * Classify data after executeQuery
     * @return ArrayList with each prediction line by line
     * @throws Exception 
     */
    public HashMap<Double, String> classifyData(String nameTableResult) throws Exception{
    
        HashMap<Double,String> ret = new HashMap<>();
        
        this.nameTableResult = nameTableResult;
        
        if(!dataToClassify.isEmpty()){
         
          for (int i = 0; i < dataToClassify.numInstances(); i++) {

            Instance newInst = dataToClassify.instance(i); 
            
            double pk = newInst.value(this.colPk);
            double predNB = model.classifyInstance(newInst);
            
            String predString = dataToClassify.classAttribute().value((int)predNB);
            newInst.setClassValue(predString);             
            ret.put(pk,predString);
            if(ret.size() >= BATCH_MAX_SIZE){
                tableResult("tableResultTest",ret);
                ret.clear();
            }
            
          }
          if(ret.size() < BATCH_MAX_SIZE){
              tableResult("tableResultTest",ret);
                ret.clear();
          }

        }
        
        return ret;       
    }
    
    public Connection getConnection() {return connection;}
    
    
    /**
     * Method for update the class of each line in database
     */
    private void tableResult(String nameTableResult,HashMap<Double,String> ret) throws SQLException{
        
        //création table avec deux col id - class
        PreparedStatement st = null;
        //prepareStatement
        
        String createTableSQL =  "CREATE TABLE IF NOT EXISTS "+nameTableResult+"("
				+ "ID REAL NOT NULL, "
				+ "CLASS VARCHAR(50) NOT NULL, "
				+ ")";
        
        st = connection.prepareStatement(createTableSQL);
        st.execute();
        
        String insertTableSQL = "INSERT INTO "+nameTableResult
				+ "(ID, CLASS) VALUES"
				+ "(?,?)";
        st = connection.prepareStatement(insertTableSQL);
        
        Set<Double> keyMap = ret.keySet();
        double[] arrayKey = new double[keyMap.size()];
        int i=0;
        for(Double d : keyMap){
            arrayKey[i]=d.doubleValue();
            i++;
        }
        for(double db : arrayKey){  
            st.setDouble(1, db);
            st.setString(2, ret.get(db));
            st.addBatch();
        }
          
        st.executeBatch();
        
        String sql = "SELECT * FROM "+nameTableResult;
        ResultSet res = stat.executeQuery(sql);
        while(res.next()){
           System.out.println(res.getDouble(1)+": "+res.getString(2)); 
        }
        
        st.close();
    }
    
    
    public void resetTableResult() throws SQLException{
        String sql = "DROP TABLE IF EXISTS "+nameTableResult;
        stat.execute(sql);
    }
    
    public String getNameTableResult(){ return this.nameTableResult;}
    
}
