package org.orbisgis.mapuce.randomForest;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.h2gis.h2spatial.ut.SpatialH2UT;
import org.h2gis.utilities.TableLocation;
import static org.hamcrest.CoreMatchers.instanceOf;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.*;
import weka.classifiers.Classifier;
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
        
        File file = File.createTempFile("mapuce", ".model");
        FileUtils.copyURLToFile(new URL(""), file);
        ClassifyData test = new ClassifyData(file.getAbsolutePath(),connection);
        
        //=================== Create the tables ============
        String sql = "DROP TABLE IF EXISTS COMMUNE";
        stat.execute(sql);
        
        sql = "CREATE TABLE COMMUNE (b_PK INT NOT NULL,"
                + "i_H_ORIGIN INT NOT NULL," +
                " i_INHAB INT NOT NULL," +
                " i_H LONG NOT NULL," +
                "i_LEVELS INT NOT NULL,"+
                "i_AREA INT NOT NULL,"+
                "i_FLOOR LONG NOT NULL,"+
                "i_VOL LONG NOT NULL,"+
                "i_COMP_B LONG NOT NULL,"+
                "i_COMP_N LONG NOT NULL,"+
                "i_COMP LONG NOT NULL,"+
                "i_FROM LONG NOT NULL,"+
                "i_CONC LONG NOT NULL,"+
                "i_DIR LONG NOT NULL,"+
                "i_PERI LONG NOT NULL,"+
                "i_WALL_A LONG NOT NULL,"+
                "i_PWALL_L LONG NOT NULL,"+
                "i_PWALL_A LONG NOT NULL,"+
                "i_NB_NEI LONG NOT NULL,"+
                "i_FWALL_L LONG NOT NULL,"+
                "i_FREE_EXT_AREA LONG NOT NULL,"+
                "i_CONTIG LONG NOT NULL,"+
                "i_PASSIV_VOL LONG NOT NULL,"+
                "i_FRACTAL LONG NOT NULL,"+
                "i_DIST_MIN LONG NOT NULL,"+
                "i_DIST_MEAN LONG NOT NULL,"+
                "i_DIST_MAX LONG NOT NULL,"+
                "i_DIR_STD LONG NOT NULL,"+
                "i_NB_POINTS LONG NOT NULL,"+
                "i_L_TOT LONG NOT NULL,"+
                "i_L_CVX LONG NOT NULL,"+
                "i_L_3M LONG NOT NULL,"+
                "b_AREA LONG NOT NULL,"+
                "b_FLOOR LONG NOT NULL,"+
                "b_VOL LONG NOT NULL,"+
                "b_H_MEAN LONG NOT NULL,"+
                "b_H_STD LONG NOT NULL,"+
                "b_COMP_N LONG NOT NULL,"+
                "b_HOLES_P LONG NOT NULL,"+
                "b_DIR LONG NOT NULL,"+
                "u_VEG_A LONG NOT NULL,"+
                "u_ROAD_A LONG NOT NULL,"+
                "u_ROAD_L LONG NOT NULL,"+
                "u_SIDEWALK_L LONG NOT NULL,"+
                "u_INHAB LONG NOT NULL,"+
                "u_HOUSE LONG NOT NULL,"+
                "u_COL_HOUSE LONG NOT NULL,"+
                "u_HOUSE_A LONG NOT NULL,"+
                "u_COL_HOUSE_A LONG NOT NULL,"+
                "u_FLOOR LONG NOT NULL,"+
                "u_COS LONG NOT NULL,"+
                "u_COMP_NWMEAN LONG NOT NULL,"+
                "u_COMP_WMEAN LONG NOT NULL,"+
                "u_CONTIG_MEAN LONG NOT NULL,"+
                "u_CONTIG_STD LONG NOT NULL,"+
                "u_DIR_STD LONG NOT NULL,"+
                "u_H_MEAN LONG NOT NULL,"+
                "u_H_STD LONG NOT NULL,"+
                "u_PASSIV_VOL_MEAN LONG NOT NULL,"+
                "u_AREA LONG NOT NULL,"+
                "u_VOL LONG NOT NULL,"+
                "u_VOL_MEAN LONG NOT NULL,"+
                "u_NB_BUILD LONG NOT NULL,"+
                "u_DIST_MIN_MEAN LONG NOT NULL,"+
                "u_DIST_MEAN_MEAN LONG NOT NULL,"+
                "u_DIST_MEAN_STD LONG NOT NULL,"+
                "u_bH_STD_MEAN LONG NOT NULL,"+
                "u_bCOMP_NWMEAN LONG NOT NULL,"+
                "u_bCOMP_WMEAN LONG NOT NULL,"+
                "u_bCOMP_STD LONG NOT NULL,"+
                "u_DIST_CENTER LONG NOT NULL,"+
                "u_BUILD_DENS LONG NOT NULL,"+
                "u_VEG_DENS LONG NOT NULL,"+
                "u_ROAD_DENS LONG NOT NULL,"+
                "u_FWALL_A LONG NOT NULL)";


        stat.execute(sql);
        //==================== End create ============
        
        //==================== Insert some rows ================
        sql = "INSERT INTO COMMUNE (b_PK,i_H_ORIGIN,i_INHAB,i_H,i_LEVELS,i_AREA,i_FLOOR,i_VOL,i_COMP_B,i_COMP_N,i_COMP,i_FROM,i_CONC,i_DIR,i_PERI,i_WALL_A,i_PWALL_L,i_PWALL_A,i_NB_NEI," +
        "i_FWALL_L,i_FREE_EXT_AREA,i_CONTIG,i_PASSIV_VOL,i_FRACTAL,i_DIST_MIN,i_DIST_MEAN,i_DIST_MAX,i_DIR_STD,i_NB_POINTS,i_L_TOT,i_L_CVX,i_L_3M,b_AREA,b_FLOOR,b_VOL,b_H_MEAN,b_H_STD,b_COMP_N," +
        "b_HOLES_P,b_DIR,u_VEG_A,u_ROAD_A,u_ROAD_L,u_SIDEWALK_L,u_INHAB,u_HOUSE,u_COL_HOUSE,u_HOUSE_A,u_COL_HOUSE_A,u_FLOOR,u_COS,u_COMP_NWMEAN,u_COMP_WMEAN,u_CONTIG_MEAN,u_CONTIG_STD," +
        "u_DIR_STD,u_H_MEAN,u_H_STD,u_PASSIV_VOL_MEAN,u_AREA,u_VOL,u_VOL_MEAN,u_NB_BUILD,u_DIST_MIN_MEAN,u_DIST_MEAN_MEAN,u_DIST_MEAN_STD,u_bH_STD_MEAN,u_bCOMP_NWMEAN,u_bCOMP_WMEAN," +
        "u_bCOMP_STD,u_DIST_CENTER,u_BUILD_DENS,u_VEG_DENS,u_ROAD_DENS,u_FWALL_A)"
               + "VALUES (1,5,2,5855253433,5,1,317,5149999851,317,5149999851,1587,5749999257,5,4400384083,5,4400384083,1,33872806,0,444022935,1,86,84,5628130831,422,8140654157,0,0,0,84,5628130831,740,"+
                    "3290654008,0,1,1,5406562982,55,9811575443,463,538842058,643,3150472358,158,178957503,4,84,563,84,563,0,317,5149999851,317,5149999851,1587,5749999257,5,0,5,4400384083,0,86,10332,5038759207,"+
                    "10438,5439612486,2852,2034197533,2954,8237773887,84,9465113283,31,121874233);";
        stat.executeUpdate(sql);
        
        sql = "INSERT INTO COMMUNE (b_PK,i_H_ORIGIN,i_INHAB,i_H,i_LEVELS,i_AREA,i_FLOOR,i_VOL,i_COMP_B,i_COMP_N,i_COMP,i_FROM,i_CONC,i_DIR,i_PERI,i_WALL_A,i_PWALL_L,i_PWALL_A,i_NB_NEI," +
                "i_FWALL_L,i_FREE_EXT_AREA,i_CONTIG,i_PASSIV_VOL,i_FRACTAL,i_DIST_MIN,i_DIST_MEAN,i_DIST_MAX,i_DIR_STD,i_NB_POINTS,i_L_TOT,i_L_CVX,i_L_3M,b_AREA,b_FLOOR,b_VOL,b_H_MEAN,b_H_STD,b_COMP_N," +
                "b_HOLES_P,b_DIR,u_VEG_A,u_ROAD_A,u_ROAD_L,u_SIDEWALK_L,u_INHAB,u_HOUSE,u_COL_HOUSE,u_HOUSE_A,u_COL_HOUSE_A,u_FLOOR,u_COS,u_COMP_NWMEAN,u_COMP_WMEAN,u_CONTIG_MEAN,u_CONTIG_STD," +
                "u_DIR_STD,u_H_MEAN,u_H_STD,u_PASSIV_VOL_MEAN,u_AREA,u_VOL,u_VOL_MEAN,u_NB_BUILD,u_DIST_MIN_MEAN,u_DIST_MEAN_MEAN,u_DIST_MEAN_STD,u_bH_STD_MEAN,u_bCOMP_NWMEAN,u_bCOMP_WMEAN," +
                "u_bCOMP_STD,u_DIST_CENTER,u_BUILD_DENS,u_VEG_DENS,u_ROAD_DENS,u_FWALL_A)"
                       + "VALUES (8,9,0,9,1,525,4399999916,525,4399999916,4728,9599999246,4,9094291046,4,492575408,1,1728441095,0,578508246,0,9645702537,53,95,3030475625,857,7274280625,26,9268984304,"+
                       "242,3420858737,1,68,3761491321,1140,8253421804,0,2825397416,0,62,1,4549457536,0,239,7666052399,401,3500467173,113,491482679,11,95,303,91,953,28,129,2823,1449999792,2823,1449999792,"+
                       "25408,3049998126,9,0,5,9255262116,0,55,10332,5038759207,10438,5439612486,2852,2034197533,2954,8237773887);";
        stat.executeUpdate(sql);

        sql = "INSERT INTO COMMUNE (b_PK,i_H_ORIGIN,i_INHAB,i_H,i_LEVELS,i_AREA,i_FLOOR,i_VOL,i_COMP_B,i_COMP_N,i_COMP,i_FROM,i_CONC,i_DIR,i_PERI,i_WALL_A,i_PWALL_L,i_PWALL_A,i_NB_NEI," +
              "i_FWALL_L,i_FREE_EXT_AREA,i_CONTIG,i_PASSIV_VOL,i_FRACTAL,i_DIST_MIN,i_DIST_MEAN,i_DIST_MAX,i_DIR_STD,i_NB_POINTS,i_L_TOT,i_L_CVX,i_L_3M,b_AREA,b_FLOOR,b_VOL,b_H_MEAN,b_H_STD,b_COMP_N," +
              "b_HOLES_P,b_DIR,u_VEG_A,u_ROAD_A,u_ROAD_L,u_SIDEWALK_L,u_INHAB,u_HOUSE,u_COL_HOUSE,u_HOUSE_A,u_COL_HOUSE_A,u_FLOOR,u_COS,u_COMP_NWMEAN,u_COMP_WMEAN,u_CONTIG_MEAN,u_CONTIG_STD," +
              "u_DIR_STD,u_H_MEAN,u_H_STD,u_PASSIV_VOL_MEAN,u_AREA,u_VOL,u_VOL_MEAN,u_NB_BUILD,u_DIST_MIN_MEAN,u_DIST_MEAN_MEAN,u_DIST_MEAN_STD,u_bH_STD_MEAN,u_bCOMP_NWMEAN,u_bCOMP_WMEAN," +
              "u_bCOMP_STD,u_DIST_CENTER,u_BUILD_DENS,u_VEG_DENS,u_ROAD_DENS,u_FWALL_A)"
                     + "VALUES (17,5,2,2571966487,5,1,235,3950000029,235,3950000029,1176,9750000145,5,346718939,5,346718939,1,3261419499,0,452491171,0,8747816716,11,72,1262847703,360,6314238513,0,0,0,72,"+
                     "1262847703,596,264238541,0,1,1,5668232813,23,2417727377,167,7610917258,524,328711067,109,9765311394,13,72,126,65,196,0,235,3950000029,235,3950000029,1176,9750000145,5,0,5,346718939,0,11,"+
                     "10332,5038759207,10438,5439612486,2852,2034197533,2954,8237773887,84,9465113283,31);";
        stat.executeUpdate(sql);

        sql = "INSERT INTO COMMUNE (b_PK,i_H_ORIGIN,i_INHAB,i_H,i_LEVELS,i_AREA,i_FLOOR,i_VOL,i_COMP_B,i_COMP_N,i_COMP,i_FROM,i_CONC,i_DIR,i_PERI,i_WALL_A,i_PWALL_L,i_PWALL_A,i_NB_NEI," +
                "i_FWALL_L,i_FREE_EXT_AREA,i_CONTIG,i_PASSIV_VOL,i_FRACTAL,i_DIST_MIN,i_DIST_MEAN,i_DIST_MAX,i_DIR_STD,i_NB_POINTS,i_L_TOT,i_L_CVX,i_L_3M,b_AREA,b_FLOOR,b_VOL,b_H_MEAN,b_H_STD,b_COMP_N," +
                "b_HOLES_P,b_DIR,u_VEG_A,u_ROAD_A,u_ROAD_L,u_SIDEWALK_L,u_INHAB,u_HOUSE,u_COL_HOUSE,u_HOUSE_A,u_COL_HOUSE_A,u_FLOOR,u_COS,u_COMP_NWMEAN,u_COMP_WMEAN,u_CONTIG_MEAN,u_CONTIG_STD," +
                "u_DIR_STD,u_H_MEAN,u_H_STD,u_PASSIV_VOL_MEAN,u_AREA,u_VOL,u_VOL_MEAN,u_NB_BUILD,u_DIST_MIN_MEAN,u_DIST_MEAN_MEAN,u_DIST_MEAN_STD,u_bH_STD_MEAN,u_bCOMP_NWMEAN,u_bCOMP_WMEAN," +
                "u_bCOMP_STD,u_DIST_CENTER,u_BUILD_DENS,u_VEG_DENS,u_ROAD_DENS,u_FWALL_A)"
                       + "VALUES (19,8,0,8,1,13,9499999976,13,9499999976,111,599999981,12,2399126413,6,7107467666,2,5468850666,0,122679343,1,144,33,7210727972,269,7685823778,16,20611724,128,1648937916,1,17,"+
                       "7004610733,155,5536885838,0,4750919943,1,2,6698162036,0,214,3621676704,430,811551325,98,9874289511,3,33,721,33,721,0,2263,1100000139,2263,1100000139,18104,880000111,8,0,5,6288668091,"+
                       "0,60,10332,5038759207,10438,5439612486,2852,2034197533,2954,8237773887,84,9465113283,31);";
        stat.executeUpdate(sql);

        sql = "INSERT INTO COMMUNE (b_PK,i_H_ORIGIN,i_INHAB,i_H,i_LEVELS,i_AREA,i_FLOOR,i_VOL,i_COMP_B,i_COMP_N,i_COMP,i_FROM,i_CONC,i_DIR,i_PERI,i_WALL_A,i_PWALL_L,i_PWALL_A,i_NB_NEI," +
                "i_FWALL_L,i_FREE_EXT_AREA,i_CONTIG,i_PASSIV_VOL,i_FRACTAL,i_DIST_MIN,i_DIST_MEAN,i_DIST_MAX,i_DIR_STD,i_NB_POINTS,i_L_TOT,i_L_CVX,i_L_3M,b_AREA,b_FLOOR,b_VOL,b_H_MEAN,b_H_STD,b_COMP_N," +
                "b_HOLES_P,b_DIR,u_VEG_A,u_ROAD_A,u_ROAD_L,u_SIDEWALK_L,u_INHAB,u_HOUSE,u_COL_HOUSE,u_HOUSE_A,u_COL_HOUSE_A,u_FLOOR,u_COS,u_COMP_NWMEAN,u_COMP_WMEAN,u_CONTIG_MEAN,u_CONTIG_STD," +
                "u_DIR_STD,u_H_MEAN,u_H_STD,u_PASSIV_VOL_MEAN,u_AREA,u_VOL,u_VOL_MEAN,u_NB_BUILD,u_DIST_MIN_MEAN,u_DIST_MEAN_MEAN,u_DIST_MEAN_STD,u_bH_STD_MEAN,u_bCOMP_NWMEAN,u_bCOMP_WMEAN," +
                "u_bCOMP_STD,u_DIST_CENTER,u_BUILD_DENS,u_VEG_DENS,u_ROAD_DENS,u_FWALL_A)"
                       + "VALUES (21,5,1,3783286071,5,1,105,7699999994,105,7699999994,528,8499999969,5,133731066,5,133731066,1,2183380103,0,53611079,0,9650547445,38,44,4174863877,222,874319386,0,0,0,44,4174863877,"+
                       "327,857431938,0,1,1,6277262342,8,3766127693,183,7364748977,512,9339626112,108,1300462555,8,44,417,42,801,0,105,7699999994,105,7699999994,528,8499999969,5,0,5,133731066,0,38,10332,5038759207,"+
                       "10438,5439612486,2852,2034197533,2954,8237773887,84,9465113283,31);";
        stat.executeUpdate(sql);

        sql = "INSERT INTO COMMUNE (b_PK,i_H_ORIGIN,i_INHAB,i_H,i_LEVELS,i_AREA,i_FLOOR,i_VOL,i_COMP_B,i_COMP_N,i_COMP,i_FROM,i_CONC,i_DIR,i_PERI,i_WALL_A,i_PWALL_L,i_PWALL_A,i_NB_NEI," +
                "i_FWALL_L,i_FREE_EXT_AREA,i_CONTIG,i_PASSIV_VOL,i_FRACTAL,i_DIST_MIN,i_DIST_MEAN,i_DIST_MAX,i_DIR_STD,i_NB_POINTS,i_L_TOT,i_L_CVX,i_L_3M,b_AREA,b_FLOOR,b_VOL,b_H_MEAN,b_H_STD,b_COMP_N," +
                "b_HOLES_P,b_DIR,u_VEG_A,u_ROAD_A,u_ROAD_L,u_SIDEWALK_L,u_INHAB,u_HOUSE,u_COL_HOUSE,u_HOUSE_A,u_COL_HOUSE_A,u_FLOOR,u_COS,u_COMP_NWMEAN,u_COMP_WMEAN,u_CONTIG_MEAN,u_CONTIG_STD," +
                "u_DIR_STD,u_H_MEAN,u_H_STD,u_PASSIV_VOL_MEAN,u_AREA,u_VOL,u_VOL_MEAN,u_NB_BUILD,u_DIST_MIN_MEAN,u_DIST_MEAN_MEAN,u_DIST_MEAN_STD,u_bH_STD_MEAN,u_bCOMP_NWMEAN,u_bCOMP_WMEAN," +
                "u_bCOMP_STD,u_DIST_CENTER,u_BUILD_DENS,u_VEG_DENS,u_ROAD_DENS,u_FWALL_A)"
                       + "VALUES (24,4,1,3747449787,4,1,105,4949999983,105,4949999983,421,9799999934,5,837281073,5,837281073,1,2394402134,0,518010986,0,9192662949,31,45,1280380492,180,5121521968,0,0,0,45,1280380492,"+
                       "286,71521952,0,1,1,6354491763,5,9665735565,184,9745011992,467,1618242111,99,8149706136,10,45,128,41,544,0,105,4949999983,105,4949999983,421,9799999934,4,0,5,837281073,0,31,10332,5038759207,"+
                       "10438,5439612486,2852,2034197533,2954,8237773887,84,9465113283,31);";
        stat.executeUpdate(sql);
        //==================== End insert =====================
        
        //Get all the data in my database
        sql = "SELECT * FROM COMMUNE";
        
        //===========ResultSet contains something==============
        ResultSet res = stat.executeQuery(sql);
        
        assertTrue("failure - ResultSet have nothing", res.next());
        res.close();
        
        //===========Instances contains something==============
        res = stat.executeQuery(sql);
        Instances inst = test.resultSetToInstances(res,"b_PK");        
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
        
        //representation of the table
        sql = "SELECT * FROM "+loc.toString();
        ResultSet r = stat.executeQuery(sql);
        int i=0;
        while(r.next()){
            i++;
        }
        assertSame("Table have more or less rows of Instances",i,nbRowsInst);
        
        //======= End =========
        sql = "DROP TABLE IF EXISTS "+loc.toString();
        stat.execute(sql);

  }
}
