package org.orbisgis.mapuce.randomForest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Set;
import org.h2gis.utilities.TableLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
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
       
    //used to optimize performance
    private static final int BATCH_MAX_SIZE = 100;
    
    private Logger LOGGER = LoggerFactory.getLogger(ClassifyData.class);
    

    /**
     * Initialize the Classifier RandomForest with a .model file
     * @param pathModel where the .model file is store
     * @param conn The connection to the database
     * @throws Exception 
     */
    public ClassifyData(String pathModel, Connection conn) throws Exception{
        
        Object[] obj = weka.core.SerializationHelper.readAll(pathModel);
        model = (RandomForest) obj[0];
        Instances inst = (Instances)obj[1];
        attClass = inst.classAttribute();

        connection = conn;
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
           int col = rs.findColumn(nameColIndex);
           nameTypeIndex = rs.getMetaData().getColumnTypeName(col);
           this.colIndex = col-1; 
        }
        catch(SQLException e){
            LOGGER.error(e.getMessage());
        }
        
        Instances inst = InstanceQuery.retrieveInstances(new InstanceQuery(),rs);
        
        //Use to define class attribute
        inst.insertAttributeAt(attClass, inst.numAttributes());
        inst.setRelationName("classification");
        inst.setClassIndex(inst.numAttributes()-1);
  
        this.dataToClassify = inst;
        return inst;
    }
    
    /**
     * Classify data after executeQuery
     * @param nameTableResult table wich is stored result
     * @throws Exception 
     */
    public void classify(String nameTableResult) throws Exception{
    
        HashMap<Object,String> ret = new HashMap<>();
        
        TableLocation nameTable = new TableLocation(nameTableResult);
        String nameIndexResult = dataToClassify.attribute(colIndex).name();
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
                tableResult(ret,nameTable,nameIndexResult);
                ret.clear();
            }
            
          }
          if(ret.size() < BATCH_MAX_SIZE){
              tableResult(ret,nameTable,nameIndexResult);
              ret.clear();
          }
          
        }   
    }
    
    
    
    /**
     * Method for update the class of each line in database
     * @param map value to insert in table result
     * @ret HashMap wich stores the id and class
     */
    private void tableResult(HashMap<Object,String> map,TableLocation tab,String nameIndex) throws SQLException{
        
        PreparedStatement st = null;
        Statement sta = connection.createStatement();

        String createTableSQL =  "CREATE TABLE IF NOT EXISTS "+tab.toString()+"("
				+ nameIndex+" "+nameTypeIndex+ " NOT NULL, "
				+ "typo VARCHAR(50) NOT NULL, "
				+ ")";
        sta.execute(createTableSQL);
        
        String insertTableSQL = "INSERT INTO "+tab.toString() + "("+nameIndex+", typo)"
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
     * Return the connection set to this class
     * @return 
     */
    public Connection getConnection(){
        return connection;
    }
}
