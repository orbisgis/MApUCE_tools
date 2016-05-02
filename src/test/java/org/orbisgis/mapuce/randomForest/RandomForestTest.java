package org.orbisgis.mapuce.randomForest;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2gis.h2spatial.ut.SpatialH2UT;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
    public void testCreateInstancesFromDB() throws SQLException, IOException {
        Statement stat = connection.createStatement();
        //Create the tables
        
    }
    
    @Test
    public void testCreateModel() throws SQLException, IOException {
        Statement stat = connection.createStatement();
        //Create the tables
        
    }

}
