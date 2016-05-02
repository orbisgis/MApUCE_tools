package org.orbisgis.mapuce.randomForest;

import java.sql.ResultSet;
import java.util.ArrayList;
import weka.classifiers.trees.RandomForest;
import weka.core.Instance;
import weka.core.Instances;
import weka.experiment.InstanceQuery;

/**
 * Class that classifies data following a SQL request
 * @author Melvin Le Gall
 */
public class Classify {

    //the model used to classifies
    RandomForest model;
    
    //Instances which is classified
    Instances dataToClassify;
    
    //Contains the class result for each line
    ArrayList<String> resultClassify;
    
    /**
     * Initialize the Classifier RandomForest with a .model file
     * @param pathModel where the .model file is store
     * @throws Exception 
     */
    public Classify(String pathModel) throws Exception{
        
        model = (RandomForest) weka.core.SerializationHelper.read(pathModel);
    }
    
    /**
     * Transform ResultSet into a usable format for Weka (Instances)
     * @param res ResultSet got after an SQL request
     * @throws Exception 
     */
    public void ResultSetToInstances(ResultSet res) throws Exception{
        
        ResultSet rst = res;
        InstanceQuery query = new InstanceQuery();
        Instances data = query.retrieveInstances(query,rst);
        System.out.println(data);
        
        dataToClassify = data;
        
    }
    
    /**
     * Execute request via a connection to the database
     * @param query query like "SELECT * FROM the_table"
     * @return ResultSet of the request
     */
    public ResultSet executeQuery(String query){
        
        return null;
    }
    
    /**
     * Classify data after executeQuery
     * @return ArrayList with each prediction line by line
     * @throws Exception 
     */
    public ArrayList<String> classifyData() throws Exception{
                  
        ArrayList<String> ret = new ArrayList<>();

        if(dataToClassify.isEmpty()){
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
     * Method for update the class of line in database
     */
    public void updateDataBase(){
        
//        for(String item : resultClassify){
//            //UPDATE DATABASE
//        }
        
    }
    
    
}
