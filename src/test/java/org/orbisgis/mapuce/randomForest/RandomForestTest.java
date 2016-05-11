package org.orbisgis.mapuce.randomForest;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import org.junit.Test;
import static org.junit.Assert.*;
import weka.core.Instances;

/**
 *
 * @author ebocher Melvin Le Gall
 */
public class RandomForestTest {
    
    @Test
    public void testCreateInstancesFromDB() throws SQLException, IOException, Exception {
        
        ClassifyData test = new ClassifyData(this.getClass().getResource("iris.model").getPath());
        Statement stat = test.getConnection().createStatement();
        //=================== Create the tables ============
        String sql = "DROP TABLE IF EXISTS IRIS ";
        stat.execute(sql);
        
        sql = "CREATE TABLE IRIS " +
                "(sepallength    REAL     NOT NULL," +
                " sepalwidth     REAL   NOT NULL, " +
                " petallength    REAL    NOT NULL, " +
                " petalwidth        REAL)";
        stat.execute(sql);
        //==================== End create ============
        
        //==================== Insert some rows ================
        sql = "INSERT INTO IRIS (sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (5.3,3.7,1.5,0.2);";
        stat.executeUpdate(sql);

        sql = "INSERT INTO IRIS (sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (5.0,3.3,1.4,0.2);";
        stat.executeUpdate(sql);

        sql = "INSERT INTO IRIS (sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (6.3,3.3,4.7,1.6);";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS (sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (4.9,2.4,3.3,1.0);";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS (sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (6.4,2.7,5.3,1.9);";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS (sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (6.8,3.0,5.5,2.1);";
        stat.executeUpdate(sql);
        //==================== End insert =====================
        
        //Get all the data in my database
        sql = "SELECT * FROM IRIS";
        
        //===========ResultSet contains something==============
        ResultSet res = test.executeQuery(sql,"sepallength");

        assertTrue("failure - ResultSet have nothing", res.next());
        res.close();
        
        //===========Instances contains something==============
        res = stat.executeQuery(sql);
                
        Instances inst = test.resultSetToInstances(res);
        inst.setClassIndex(0);
        HashMap<Double, String> map = test.classifyData("tableResultTest");
        
        assertNotNull("Instances was null",inst.isEmpty());
        
        //===========Number of features=============
        
        ResultSetMetaData rsmd = res.getMetaData();
        //add one because the class are not represent in the table
        int columnsNumber = rsmd.getColumnCount()+1;
        int featureInst = inst.numAttributes();
        
        assertSame("Not the same number of features",columnsNumber,featureInst);
        
        
        /*
        //===========Compare each value for adress============
        res.close();
        res = stat.executeQuery(sql);
        //Compare value of each adress
        int j=0;
        while (res.next()) {
            //assertSame("should be same", res.getString(4), inst.get(j).stringValue(3));
            j++;
        }
       */
        
        //===========Number of rows=============
        res.close();
        res = stat.executeQuery(sql);
        
        int nbRowsInst = inst.numInstances();
        int nbRowsResultSet = 0;
        while (res.next()) { nbRowsResultSet++; }    
        assertSame("Not the same number of rows",nbRowsResultSet,nbRowsInst);
        
        
        //======= End =========
        stat.close();
        test.resetTableResult();
        test.getConnection().close();
        
        
    }
    
    @Test
    public void testCreateModel() throws SQLException, IOException, Exception {

        String arffFile=this.getClass().getResource("iris.arff").getPath();
        String modelFile=this.getClass().getResource("iris.model").getPath();         
        BuildModel model = new BuildModel(arffFile,modelFile,4,true);
        Object[] obj= model.getClassifier();
        model.evaluate();
        
    }

}
