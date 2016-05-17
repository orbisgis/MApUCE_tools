package org.orbisgis.mapuce.randomForest;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Assert;
import org.h2gis.h2spatial.ut.SpatialH2UT;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.*;
import weka.core.Instances;

/**
 *
 * @author ebocher
 * @author Melvin Le Gall
 */
public class RandomForestTest {
    
    private static Connection connection;
    private static final String DB_NAME = "RandomForestTest";
    private Statement stat;

    
    @BeforeClass
    public static void tearUp() throws Exception {
        // Keep a connection alive to not close the DataBase on each unit test
        connection = SpatialH2UT.openSpatialDataBase(DB_NAME);
        if(connection == null){
           connection = SpatialH2UT.createSpatialDataBase(DB_NAME);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        connection.close();
    }
    
    @Before
    public void setUpStatement() throws Exception {
        stat = connection.createStatement();
    }

    @After
    public void tearDownStatement() throws Exception {
        stat.close();
    }
    
    @Test
    public void testCreateModel() throws SQLException, IOException, Exception {
    
        String arffFile=this.getClass().getResource("iris.arff").getPath();
        String modelFile=this.getClass().getResource("iris.model").getPath();         
        BuildModel model = new BuildModel(arffFile,modelFile,4);
        Object[] obj= model.getClassifier();
        model.evaluate();
        
    }
    
    @Test
    public void testCreateInstancesFromDB() throws SQLException, IOException, Exception {
        
        ClassifyData test = new ClassifyData(this.getClass().getResource("iris.model").getPath(),connection);
        
        //=================== Create the tables ============
        String sql = "DROP TABLE IF EXISTS IRIS ";
        stat.execute(sql);
        
        sql = "CREATE TABLE IRIS " +
                "(id INT NOT NULL,"
                + "sepallength    FLOAT    NOT NULL," +
                " sepalwidth     FLOAT    NOT NULL," +
                " petallength    FLOAT    NOT NULL," +
                " petalwidth     FLOAT    NOT NULL)";
        stat.execute(sql);
        //==================== End create ============
        
        //==================== Insert some rows ================
        sql = "INSERT INTO IRIS (id,sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (1,5.3,3.7,1.5,0.2);";
        stat.executeUpdate(sql);

        sql = "INSERT INTO IRIS (id,sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (2,5.0,3.3,1.4,0.2);";
        stat.executeUpdate(sql);

        sql = "INSERT INTO IRIS (id,sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (3,6.3,3.3,4.7,1.6);";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS (id,sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (4,4.9,2.4,3.3,1.0);";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS (id,sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (5,6.4,2.7,5.3,1.9);";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS (id,sepallength,sepalwidth,petallength,petalwidth) "
               + "VALUES (6,6.8,3.0,5.5,2.1);";
        stat.executeUpdate(sql);
        //==================== End insert =====================
        
        //Get all the data in my database
        sql = "SELECT * FROM IRIS";
        
        //===========ResultSet contains something==============
        ResultSet res = stat.executeQuery(sql);
        
        assertTrue("failure - ResultSet have nothing", res.next());
        res.close();
        
        //===========Instances contains something==============
        res = stat.executeQuery(sql);
        Instances inst = test.resultSetToInstances(res,"id");        
        assertNotNull("Instances was null",inst.isEmpty());
        res.close();

        //===========Compare each value ============
        //Between the ResultSet transformation into Instances
        res = stat.executeQuery(sql);
        int j=0;
        while (res.next()) {
            for(int i=0; i < inst.numAttributes()-1;i++){
               Assert.assertEquals(res.getDouble(i+1), inst.get(j).value(i),0.0000001f); 
            }
            j++;
        }
        
        //===========Number of rows=============
        res = stat.executeQuery(sql);
        int nbRowsInst = inst.numInstances();
        int nbRowsResultSet = 0;
        while (res.next()) { nbRowsResultSet++; }    
        assertSame("Not the same number of rows",nbRowsResultSet,nbRowsInst);

        //=======Classify data======
        test.classify("tableResultTest");
        test.printTableResult();
        
        //======= End =========
        test.resetTableResult();

    }
}
