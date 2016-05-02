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
import org.h2gis.h2spatial.ut.SpatialH2UT;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import weka.core.Instances;
import weka.experiment.InstanceQuery;

/**
 *
 * @author ebocher
 */
public class RandomForestTest {

    private static Connection connection;
    private static final String DB_NAME = "RandomForestTest";
    private Statement st;

    
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
        st = connection.createStatement();
    }

    @After
    public void tearDownStatement() throws Exception {
        st.close();
    }
    
    
    @Test
    public void testCreateInstancesFromDB() throws SQLException, IOException, Exception {
        Statement stat = connection.createStatement();
        //Create the tables
        String sql = "CREATE TABLE COMPANY " +
                      "(ID INT PRIMARY KEY     NOT NULL," +
                      " NAME           TEXT    NOT NULL, " +
                      " AGE            INT     NOT NULL, " +
                      " ADDRESS        VARCHAR(50), " +
                      " SALARY         REAL)";
        stat.executeUpdate(sql);

        sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
               + "VALUES (2, 'Allen', 25, 'Texas', 15000.00 );";
        stat.executeUpdate(sql);

        sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
               + "VALUES (3, 'Teddy', 23, 'Norway', 20000.00 );";
        stat.executeUpdate(sql);

        sql = "INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) "
               + "VALUES (4, 'Mark', 25, 'Rich-Mond', 65000.00 );";
        stat.executeUpdate(sql);
        
        
        sql = "SELECT * FROM COMPANY";
        
        //===========ResultSet contains something==============
        ResultSet res = stat.executeQuery(sql);

        assertTrue("failure - ResultSet have nothing", res.next());
        res.close();
        
        //===========Instances contains something==============
        res = stat.executeQuery(sql);
        String pathname = "/home/mlgall/Documents/RandomForest/data/RandomForest.model";
        Classify test = new Classify(pathname);
        Instances inst = test.resultSetToInstances(res);
        inst.setClassIndex(0);
        
        //examples of representation of Instances
        System.err.println(inst);
        
        assertNotNull("Instances was null",inst.isEmpty());
        
        
        //===========Number of features=============
        ResultSetMetaData rsmd = res.getMetaData();
        int columnsNumber = rsmd.getColumnCount();
        int featureInst = inst.numAttributes();
        
        assertSame("Not the same number of features",columnsNumber,featureInst);
        
        
        
        //===========Compare each value for adress============
        res.close();
        res = stat.executeQuery(sql);
        //Compare value of each adress
        int j=0;
        while (res.next()) {
            assertSame("should be same", res.getString(4), inst.get(j).stringValue(3));
            j++;
        }
        
        //===========Number of rows=============
        int nbRowsResultSet = j;
        int nbRowsInst = inst.numInstances();
        assertSame("Not the same number of rows",nbRowsResultSet,nbRowsInst);
        
        
        stat.close();
        
        
    }
    
    @Test
    public void testCreateModel() throws SQLException, IOException {
        Statement stat = connection.createStatement();
        //Create the tables
        
    }

}
