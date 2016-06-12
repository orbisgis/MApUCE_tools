package org.orbisgis.mapuce.randomForest;


import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Assert;
import org.h2gis.h2spatial.ut.SpatialH2UT;
import org.h2gis.utilities.TableLocation;
import static org.hamcrest.CoreMatchers.instanceOf;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.*;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.converters.ConverterUtils;

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
        connection = SpatialH2UT.createSpatialDataBase(DB_NAME);
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
        
        model.evaluate();
        
        //========= Classifier of .model is RandomForest===========
        Object clas= model.getClassifier();
        assertThat(clas, instanceOf(RandomForest.class));
        
        //======= Header the same as arff File===========
        //Instances model
        Instances mod = model.getHeader();
        //Instances arff file
        ConverterUtils.DataSource sourceTestValue = new ConverterUtils.DataSource(arffFile);
    	Instances arff = sourceTestValue.getDataSet();
    	arff.setClassIndex(4);
        
        assertEquals("One have more attribute",arff.numAttributes(),mod.numAttributes());
        assertEquals("One have more classes available",arff.numClasses(),mod.numClasses());
        
        //all the attributes have the same name in .model and Arff File
        for(int i=0; i < arff.numAttributes();i++){
            String nameArff = arff.attribute(i).name();
            String nameMod= mod.attribute(i).name();
            assertEquals("Not the same name for this attribute",nameArff,nameMod);
            
            int typeMod = mod.attribute(i).type();
            int typeArff = arff.attribute(i).type();
            assertEquals("Not the same type",typeMod,typeArff);
        }
    }
    
    @Test
    public void testCreateInstancesFromDB() throws SQLException, IOException, Exception {
        
        String modelFile = this.getClass().getResource("iris.model").getPath();
        ClassifyData test = new ClassifyData(modelFile,connection);
        
        //=================== Create the tables ============
        String sql = "DROP TABLE IF EXISTS IRIS";
        stat.execute(sql);
        
        sql = "CREATE TABLE IRIS (id INT, "
                + "sepallength DOUBLE,"+
                "sepalwidth DOUBLE,"+
                "petallength DOUBLE,"+
                "petalwidth DOUBLE)";


        stat.execute(sql);
        //==================== End create ============
        
        //==================== Insert some rows ================
        sql = "INSERT INTO IRIS(ID,SEPALLENGTH,SEPALWIDTH,PETALLENGTH,PETALWIDTH) VALUES (1,5.3,3.7,1.5,0.2)";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS(ID,SEPALLENGTH,SEPALWIDTH,PETALLENGTH,PETALWIDTH) VALUES (2,5,3.3,1.4,0.2)";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS(ID,SEPALLENGTH,SEPALWIDTH,PETALLENGTH,PETALWIDTH) VALUES (3,6.3,3.3,4.7,1.6)";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS(ID,SEPALLENGTH,SEPALWIDTH,PETALLENGTH,PETALWIDTH) VALUES (4,4.9,2.4,3.3,1.0)";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS(ID,SEPALLENGTH,SEPALWIDTH,PETALLENGTH,PETALWIDTH) VALUES (5,6.4,2.7,5.3,1.9)";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO IRIS(ID,SEPALLENGTH,SEPALWIDTH,PETALLENGTH,PETALWIDTH) VALUES (6,6.8,3.0,5.5,2.1)";
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
        Instances inst = test.resultSetToInstances(res,"ID");        
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
        TableLocation loc = new TableLocation("tableResultTest");
        
        sql = "SELECT * FROM "+loc.toString();
        ResultSet r = stat.executeQuery(sql);
        int i=0;
        while(r.next()){
            i++;
            assertNotNull("some typo have null value",r.getObject("typo"));
        }
        assertSame("Table have more or less rows of Instances",i,nbRowsInst);
        
        //======= End =========
        sql = "DROP TABLE IF EXISTS "+loc.toString();
        stat.execute(sql);

  }
}
