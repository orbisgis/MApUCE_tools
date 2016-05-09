package org.orbisgis.mapuce.randomForest;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;
import org.h2gis.h2spatial.ut.SpatialH2UT;
import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Utils;
import weka.experiment.InstanceQuery;

/**
 * Class that classifies data following a SQL request
 * @author Melvin Le Gall
 */
public class Classifier {

    //the model used to classifies
    private RandomForest model;
    
    //Instances which is classified
    private Instances dataToClassify;
    
    //Contains the class result for each line
    private ArrayList<String> resultClassify;

    //Connection to execute query
    private static Connection connection;
    
    //temporary DB for the Test
    private static final String DB_NAME = "RandomForestTest";
    
    /**
     * Initialize the Classifier RandomForest with a .model file
     * @param pathModel where the .model file is store
     * @throws Exception 
     */
    public Classifier(String pathModel) throws Exception{
        
        model = (RandomForest) weka.core.SerializationHelper.read(pathModel);
        
        connection = SpatialH2UT.openSpatialDataBase(DB_NAME);
        if(connection == null){
           connection = SpatialH2UT.createSpatialDataBase(DB_NAME);
        }
    }
    
    /**
     * Initialize the Classifier RandomForest with a .model file
     * @param pathModel where the .model file is store
     * @throws Exception 
     */
    public Classifier() throws Exception{
        
        model = null;
        
        connection = SpatialH2UT.openSpatialDataBase(DB_NAME);
        if(connection == null){
           connection = SpatialH2UT.createSpatialDataBase(DB_NAME);
        }
    }
    
    /**
     * Transform ResultSet into a usable format for Weka (Instances)
     * Work for simple data type
     * @param res ResultSet got after an SQL request
     * @throws Exception 
     */
    public Instances resultSetToInstances(ResultSet rs) throws Exception{
    
        InstanceQuery query = new InstanceQuery();
        System.out.println(query.getCustomPropsFile());
        Instances inst = InstanceQuery.retrieveInstances(new InstanceQuery(),rs);
        
        this.dataToClassify = inst;
        return inst;
    }
    

    /**
     * Transform ResultSet into a usable format for Weka (Instances)
     * Method who manage CLOB data type
     * @param res ResultSet got after an SQL request
     * @throws Exception 
     */
    public Instances resultSetToInstances(ResultSet rs, String temp) throws Exception{
        
        ResultSetMetaData md = rs.getMetaData(); 
        // Determine structure of the instances
        int numAttributes = md.getColumnCount();
        int[] attributeTypes = new int[numAttributes];
        @SuppressWarnings("unchecked")
        Hashtable<String, Double>[] nominalIndexes = new Hashtable[numAttributes];
        @SuppressWarnings("unchecked")  
        ArrayList<String>[] nominalStrings = new ArrayList[numAttributes];

        for (int i = 1; i <= numAttributes; i++) {

            String typeName = md.getColumnTypeName(i);
          if(typeName.equals("CLOB")
                  || typeName.equals("TEXT")
                        || typeName.equals("STRING")
                            || typeName.equals("VARCHAR")){
              attributeTypes[i - 1] = Attribute.NOMINAL;
              nominalIndexes[i - 1] = new Hashtable<String, Double>();
              nominalStrings[i - 1] = new ArrayList<String>();
          }  
          else if(typeName.equals("BOOL")){
              // System.err.println("boolean --> nominal");
              attributeTypes[i - 1] = Attribute.NOMINAL;
              nominalIndexes[i - 1] = new Hashtable<String, Double>();
              nominalIndexes[i - 1].put("false", new Double(0));
              nominalIndexes[i - 1].put("true", new Double(1));
              nominalStrings[i - 1] = new ArrayList<String>();
              nominalStrings[i - 1].add("false");
              nominalStrings[i - 1].add("true");  
          }
          else if(typeName.equals("DOUBLE")
                    || typeName.equals("BYTE")
                        || typeName.equals("SHORT")
                            || typeName.equals("INTEGER")
                                || typeName.equals("LONG")
                                    || typeName.equals("FLOAT")
                                        || typeName.equals("REAL")){
              
            attributeTypes[i - 1] = Attribute.NUMERIC;  
          }
          else if(typeName.equals("TIME")
                    || typeName.equals("TIMESTAMP")
                         || typeName.equals("DATE")){
            attributeTypes[i - 1] = Attribute.DATE; 
          }
          else{
              attributeTypes[i - 1] = Attribute.STRING;
          }
        }
        
        Vector<String> columnNames = new Vector<String>();
        for (int i = 0; i < numAttributes; i++) {
          columnNames.add(md.getColumnLabel(i + 1));
        }
        
        ArrayList<Instance> instances = new ArrayList<Instance>();
        int rowCount = 0;
        while (rs.next()) {
            
          double[] vals = new double[numAttributes];
          
          for (int i = 1; i <= numAttributes; i++) {
            
            String typeName = md.getColumnTypeName(i);
            
            if(typeName.equals("STRING") || typeName.equals("VARCHAR")){
                String str = rs.getString(i);             
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                Double index = nominalIndexes[i - 1].get(str);
                if (index == null) {
                   
                  index = new Double(nominalStrings[i - 1].size());
                  nominalIndexes[i - 1].put(str, index);
                  nominalStrings[i - 1].add(str);
                }               
                vals[i - 1] = index.doubleValue();
                
              }
            }
            else if(typeName.equals("CLOB")){
              
              Clob clo = rs.getClob(i);
              String str = clo.getSubString(1, (int)clo.length());
              
              if (rs.wasNull()) {         
                vals[i - 1] = Utils.missingValue();
              } else {            
                Double index = nominalIndexes[i - 1].get(str); 
                if (index == null) {
                  index = new Double(nominalStrings[i - 1].size());
                  nominalIndexes[i - 1].put(str, index);
                  nominalStrings[i - 1].add(str);
                  
                }                
                vals[i - 1] = index.doubleValue();                
              }
            }
            else if(typeName.equals("REAL") || typeName.equals("DOUBLE")){
                // BigDecimal bd = rs.getBigDecimal(i, 4);
              double dd = rs.getDouble(i);
              // Use the column precision instead of 4?
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                // newInst.setValue(i - 1, bd.doubleValue());
                vals[i - 1] = dd;
              } 
            }
            else if(typeName.equals("BOOLEAN")){
              boolean boo = rs.getBoolean(i);              
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                vals[i - 1] = (boo ? 1.0 : 0.0);
              }
            }
            else if(typeName.equals("BYTE")){
                byte by = rs.getByte(i);
                if (rs.wasNull()) {
                    vals[i - 1] = Utils.missingValue();
                } else {
                    vals[i - 1] = by;
                }
            }
            else if(typeName.equals("SHORT")){
              short sh = rs.getShort(i);
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                vals[i - 1] = sh;
              }
            }
            else if(typeName.equals("INTEGER")){         
              int in = rs.getInt(i);
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                vals[i - 1] = in;
              }  
            } 
            else if(typeName.equals("LONG")){
              long lo = rs.getLong(i);
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                vals[i - 1] = lo;
              }  
            }
            else if(typeName.equals("FLOAT")){
              float fl = rs.getFloat(i);
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                vals[i - 1] = fl;
              }  
            }
            else if(typeName.equals("DATE")){
              Date date = rs.getDate(i);
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                // TODO: Do a value check here.
                vals[i - 1] = date.getTime();
              } 
            }
            else if(typeName.equals("TIME")){
               Time time = rs.getTime(i);
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                // TODO: Do a value check here.
                vals[i - 1] = time.getTime();
              } 
            }
            else if(typeName.equals("TIMESTAMP")){
              Timestamp ts = rs.getTimestamp(i);
              if (rs.wasNull()) {
                vals[i - 1] = Utils.missingValue();
              } else {
                vals[i - 1] = ts.getTime();
              }
            }
            else{
               vals[i - 1] = Utils.missingValue(); 
            }
            
          }
          Instance newInst = new DenseInstance(1.0, vals);
          

          instances.add(newInst);
          rowCount++;
        }
        
        
        ArrayList<Attribute> attribInfo = new ArrayList<Attribute>();
        for (int i = 0; i < numAttributes; i++) {
          
          
          String attribName = columnNames.get(i);
          switch (attributeTypes[i]) {
          case Attribute.NOMINAL:
            attribInfo.add(new Attribute(attribName, nominalStrings[i]));
            break;
          case Attribute.NUMERIC:
            attribInfo.add(new Attribute(attribName));
            break;
          case Attribute.STRING:
            Attribute att = new Attribute(attribName, (ArrayList<String>) null);
            attribInfo.add(att);
            for (int n = 0; n < nominalStrings[i].size(); n++) {
              att.addStringValue(nominalStrings[i].get(n));
            }
            break;
          case Attribute.DATE:
            attribInfo.add(new Attribute(attribName, (String) null));
            break;
          default:
            throw new Exception("Unknown attribute type");
          }
        }
        Instances result = new Instances("QueryResult", attribInfo,
          instances.size());
        for (int i = 0; i < instances.size(); i++) {
          result.add(instances.get(i));
        }

        this.dataToClassify = result;
        return result;
        
 
    }
    
    /**
     * Execute request via a connection to the database 
     * @param query query like "SELECT * FROM the_table"
     * @return ResultSet of the request
     */
    public ResultSet executeQuery(String query) throws SQLException{
        
        Statement stat = connection.createStatement();
        ResultSet res = stat.executeQuery(query);
        
        return res;
    }
    
    /**
     * Classify data after executeQuery
     * @return ArrayList with each prediction line by line
     * @throws Exception 
     */
    public ArrayList<String> classifyData() throws Exception{
                  
        ArrayList<String> ret = new ArrayList<>();

        if(!dataToClassify.isEmpty()){
          for (int i = 0; i < dataToClassify.numInstances(); i++) {

            Instance newInst = dataToClassify.instance(i); 
            double predNB = model.classifyInstance(newInst);
            String predString = dataToClassify.classAttribute().value((int)predNB);

            newInst.setClassValue(predString);             
            ret.add(predString); 
            }  
        }
        
        return resultClassify =ret;       
    }
    
    
    /**
     * Method for update the class of each line in database
     */
    public void updateDataBase() throws SQLException{
        
        Statement st = connection.createStatement();
        
        for(String item : resultClassify){
            //UPDATE DATABASE

        }
        
    }
    
    
}
