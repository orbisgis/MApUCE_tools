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
import org.slf4j.LoggerFactory;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.DenseInstance;
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
    private int colIndex;
    private String nameTypeIndex;

    //Connection at DB
    private static Connection connection;
    
    //Statement for execute query
    private static Statement stat;
    
    //name of the table where the results are stored
    private static String nameTableResult;
    
    //used to optimize performance
    private static final int BATCH_MAX_SIZE = 100;
    

    /**
     * Initialize the Classifier RandomForest with a .model file
     * @param pathModel where the .model file is store
     * @throws Exception 
     */
    public ClassifyData(String pathModel, Connection conn) throws Exception{
        
        Object[] obj = weka.core.SerializationHelper.readAll(pathModel);
        model = (RandomForest) obj[0];
        Instances inst = (Instances)obj[1];
        attClass = inst.classAttribute();

        connection = conn;
        stat = connection.createStatement();
    }
    
    /**
     * Initialize the Classifier RandomForest with a fixed model build for the MApUCE project
     * @throws Exception 
     */
    public ClassifyData(Connection conn) throws Exception{
        
        Object[] obj = weka.core.SerializationHelper.readAll(this.getClass().getResource("MApUCE.model").getPath());
        model = (RandomForest) obj[0];
        Instances inst = (Instances)obj[1];
        attClass = inst.classAttribute();
        
        connection = conn;
        stat = connection.createStatement();
    }
    
    /**
     * Transform ResultSet into a usable format for Weka (Instances)
     * Work for simple data type
     * @param rs ResultSet got after an SQL request
     * @param nameColIndex Name of the collumn used to trace each line
     * @throws Exception 
     */
    public Instances resultSetToInstances(ResultSet rs,String nameColIndex) throws Exception{

        try{
           nameTypeIndex = rs.getMetaData().getColumnTypeName(rs.findColumn(nameColIndex));
           this.colIndex = rs.findColumn(nameColIndex)-1; 
        }
        catch(SQLException e){
            LoggerFactory.getLogger(ClassifyData.class).error(e.getMessage());
        }
        
        
        Instances inst = InstanceQuery.retrieveInstances(new InstanceQuery(),rs);
        
        //Use to define class attribute
        inst.insertAttributeAt(attClass, inst.numAttributes());
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
     * Classify data after executeQuery
     * @param nameTableResult table wich is stored result
     * @throws Exception 
     */
    public void classify(String nameTableResult) throws Exception{
    
        HashMap<Object,String> ret = new HashMap<>();
        
        this.nameTableResult = nameTableResult;
        
        if(!dataToClassify.isEmpty()){
         
          for (int i = 0; i < dataToClassify.numInstances(); i++) {

            Instance newInst = dataToClassify.instance(i); 
            
            Object index = newInst.value(this.colIndex);
            
            DenseInstance inst = new DenseInstance(1, newInst.toDoubleArray());
            inst.deleteAttributeAt(this.colIndex);
            inst.setDataset(dataToClassify);
            
            double predNB = model.classifyInstance(inst);
            String predString = dataToClassify.classAttribute().value((int)predNB);
            newInst.setClassValue(predString);
            
            ret.put(index,predString);
            if(ret.size() >= BATCH_MAX_SIZE){
                tableResult(ret);
                ret.clear();
            }
            
          }
          if(ret.size() < BATCH_MAX_SIZE){
              tableResult(ret);
              ret.clear();
          }
          
        }   
    }
    
    
    
    /**
     * Method for update the class of each line in database
     * @param map value to insert in table result
     * @ret HashMap wich stores the id and class
     */
    private void tableResult(HashMap<Object,String> map) throws SQLException{
        
        PreparedStatement st = null;
        
        String createTableSQL =  "CREATE TABLE IF NOT EXISTS "+nameTableResult+"("
				+ "INDEX "+nameTypeIndex+ " NOT NULL, "
				+ "CLASS VARCHAR(50) NOT NULL, "
				+ ")";
        
        st = connection.prepareStatement(createTableSQL);
        st.execute();
        
        String insertTableSQL = "INSERT INTO "+nameTableResult + "(INDEX, CLASS)"
                                +"VALUES (?,?)";
        st = connection.prepareStatement(insertTableSQL);
        
        Set<Object> keyMap = map.keySet();
        Object[] arrayKey = new Object[keyMap.size()];
        int i=0;
        for(Object d : keyMap){
            arrayKey[i]=d;
            i++;
        }
        for(Object db : arrayKey){  
            st.setObject(1,db);
            st.setString(2, map.get(db));
            st.addBatch();
        }
          
        st.executeBatch();
        st.close();
    }
    
    /**
     * reset the table result use for this classification 
     * @throws SQLException 
     */
    public void resetTableResult() throws SQLException{
        String sql = "DROP TABLE IF EXISTS "+nameTableResult;
        stat.execute(sql);
    }
    
    /**
     * 
     * @return name of table result 
     */
    public String getNameTableResult(){ return this.nameTableResult;}
    
    /**
     * Print the table result in the logger
     * @throws SQLException 
     */
    public void printTableResult() throws SQLException{
        
        String sql = "SELECT * FROM "+nameTableResult;
        ResultSet res = stat.executeQuery(sql);
        String result="";
        while(res.next()){
            result+="ID: "+res.getObject(1)+" Class: "+res.getString(2)+"\n";
        }
        LoggerFactory.getLogger(ClassifyData.class).info("\n"+result);
    }
    
}
