
import org.orbisgis.wpsgroovyapi.input.*
import org.orbisgis.wpsgroovyapi.output.*
import org.orbisgis.wpsgroovyapi.process.*
import javax.swing.JOptionPane
import org.orbisgis.mapuce.randomForest.*
import java.sql.ResultSet
import java.sql.Statement
import java.io.File
import java.net.URL
import java.sql.Connection
import org.apache.commons.io.FileUtils


/**
 * This process is used to classify the building and USR features using a RandomForest algorithm.
 *
 *
 * @return The table whith an ID and the class of each buildings
 *
 * @author Melvin Le Gall
 * @author Erwan Bocher
 */
@Process(title = "Classify the BUILDING and USR features",
    resume = "This process is used to classify the building and USR features using a RandomForest algorithm",
    keywords = "Vector,MAPuCE")
def processing() {

    logger.info "Download the MAPuCE model used to classify the data."

    //Do not download the file is already exist
    File file = new File(System.getProperty("user.home") + "/mapuce/mapuce-1.0.model");
    
    if(!file.exists()){
        FileUtils.copyURLToFile(new URL("https://github.com/orbisgis/MApUCE_tools/raw/master/model/mapuce-1.0.model"), file)   
    }  

    /**
     * Join between BLOCK_INDICATORS BUILDING_INDICATORS and USR_INDICATORS
     */
    logger.info "Merge all morphological indicators available on BUILDING, BLOCK and USR levels at the BUILDING scale."
    String req = " SELECT a.PK AS I_PK,a.HAUTEUR_ORIGIN AS i_H_Origin,a.INSEE_INDIVIDUS AS i_INHAB,"+
    "a.HAUTEUR AS i_H,a.NB_NIV AS i_LEVELS,a.AREA AS i_AREA,a.FLOOR_AREA AS i_FLOOR,a.VOL AS i_VOL,a.COMPACITY_R AS i_COMP_B,a.COMPACITY_N AS i_COMP_N,"+
    "a.COMPACTNESS AS i_COMP,a.FORM_FACTOR AS i_FORM,a.CONCAVITY AS i_CONC, a.MAIN_DIR_DEG AS i_DIR,a.B_FLOOR_LONG AS i_PERI,a.B_WALL_AREA AS i_WALL_A,"+
    "a.P_WALL_LONG AS i_PWALL_L,a.P_WALL_AREA AS i_PWALL_A,a.NB_NEIGHBOR AS i_Nb_NEI,a.FREE_P_WALL_LONG AS i_FWALL_L,a.FREE_EXT_AREA AS i_FREE_EXT_AREA,"+
    "a.CONTIGUITY AS i_CONTIG,a.P_VOL_RATIO AS i_PASSIV_VOL,a.FRACTAL_DIM AS i_FRACTAL,a.MIN_DIST AS i_DIST_MIN,a.MEAN_DIST AS i_DIST_MEAN,a.MAX_DIST AS i_DIST_MAX,"+
    "a.STD_DIST AS i_DIST_STD,a.NUM_POINTS AS i_Nb_POINTS,a.L_TOT AS i_L_TOT,a.L_CVX AS i_L_CVX,a.L_3M AS i_L_3M,b.AREA AS b_AREA,b.FLOOR_AREA AS b_FLOOR,"+
    "b.VOL AS b_VOL,b.H_MEAN  AS b_H_MEAN,b.H_STD AS b_H_STD,b.COMPACITY AS b_COMP_N,b.HOLES_PERCENT AS b_HOLES_P,b.MAIN_DIR_DEG AS b_DIR,u.VEGETATION_SURFACE AS u_VEG_A,u.ROUTE_SURFACE AS u_ROAD_A,u.ROUTE_LONGUEUR AS u_ROAD_L,u.TROTTOIR_LONGUEUR AS u_SIDEWALK_L,"+
    "u.INSEE_INDIVIDUS AS u_INHAB,u.INSEE_MENAGES AS u_HOUSE,u.INSEE_MEN_COLL AS u_COL_HOUSE,u.INSEE_MEN_SURF AS u_HOUSE_A,u.insee_surface_collectif AS u_COL_HOUSE_A,"+
    "u.FLOOR AS u_FLOOR,u.FLOOR_RATIO AS u_COS,u.COMPAC_MEAN_NW AS u_COMP_NWMEAN,u.COMPAC_MEAN_W AS u_COMP_WMEAN,u.CONTIG_MEAN AS u_CONTIG_MEAN,u.CONTIG_STD AS u_CONTIG_STD,"+
    "u.MAIN_DIR_STD AS u_DIR_STD,u.H_MEAN AS u_H_MEAN,u.H_STD AS u_H_STD,u.P_VOL_RATIO_MEAN AS u_PASSIV_VOL_MEAN,u.B_AREA AS u_AREA,u.B_VOL AS u_VOL,"+
    "u.B_VOL_M AS u_VOL_MEAN,u.BUILD_NUMB AS u_Nb_BUILD,u.MIN_M_DIST AS u_DIST_MIN_MEAN,u.MEAN_M_DIST AS u_DIST_MEAN_MEAN,u.MEAN_STD_DIST AS u_DIST_MEAN_STD,"+
    "u.B_STD_H_MEAN AS u_bH_STD_MEAN,u.B_M_NW_COMPACITY AS u_bCOMP_NWMEAN,u.B_M_W_COMPACITY AS u_bCOMP_WMEAN,u.B_STD_COMPACITY AS u_bCOMP_STD,u.DIST_TO_CENTER AS u_DIST_CENTER,"+
    "u.BUILD_DENS AS u_BUILD_DENS,u.VEGET_DENS AS u_VEG_DENS,u.ROAD_DENS AS u_ROAD_DENS,u.EXT_ENV_AREA AS u_FWALL_A "+
    "FROM BUILDING_INDICATORS AS A "+
    "JOIN USR_INDICATORS AS U ON a.PK_USR = u.PK "+
    "JOIN BLOCK_INDICATORS b "+
    "ON a.PK_USR = b.PK_USR AND a.PK_BLOCK = b.PK_BLOCK"
    
    
    Connection  conn = sql.createConnection();
    ClassifyData cla = new ClassifyData(file.getAbsolutePath(),conn)
 
  
    Statement sta = conn.createStatement()
    ResultSet rs = sta.executeQuery(req) 

    //Transform ResultSet into Instances  
    logger.info "Prepare the data to apply classification"
    cla.resultSetToInstances(rs,"I_PK")

    //Make the classification and create the table TYPO_RESULT  
    logger.info "Classify each buildings."
    sql.execute "DROP TABLE IF EXISTS TYPO_RESULT" 
    cla.classify("TYPO_RESULT")

    sql.execute "CREATE INDEX ON TYPO_RESULT(I_PK)"  
    sql.execute "DROP TABLE IF EXISTS BUILDING_TYPO"
    sql.execute "CREATE TABLE BUILDING_TYPO AS SELECT a.THE_GEOM,a.PK,a.PK_USR,b.typo,a.AREA FROM BUILDING_INDICATORS a,TYPO_RESULT b WHERE a.PK = b.I_PK"
  
    logger.info "Compute the classification for each USR"
    sql.execute "CREATE INDEX ON BUILDING_TYPO(PK)"
    sql.execute "CREATE TABLE SUM_AREA_USR AS SELECT PK_USR,TYPO,SUM(AREA) AS AREA_USR FROM BUILDING_TYPO GROUP BY PK_USR,TYPO"
    sql.execute "CREATE TABLE TOP2_AREA AS select *,(SELECT COUNT(*) FROM SUM_AREA_USR as b WHERE b.PK_USR=a.PK_USR AND b.AREA_USR>=a.AREA_USR ) as rank FROM SUM_AREA_USR AS a"+
        " WHERE (SELECT COUNT(*) FROM SUM_AREA_USR AS b WHERE b.PK_USR=a.PK_USR AND b.AREA_USR>=a.AREA_USR ) <= 2"        
    sql.execute "CREATE TABLE  USR_TYPO_WITH_NULL_VALUE AS select DISTINCT a.THE_GEOM,a.PK,c.TYPO,"+
        "CASE WHEN b.RANK=1 THEN c.TYPO END AS MAJO,CASE WHEN b.RANK=2 THEN c.TYPO END AS SECOND"+
        " FROM USR_INDICATORS a, SUM_AREA_USR c,TOP2_AREA b"+
        " WHERE a.PK = c.PK_USR AND c.PK_USR = b.PK_USR AND c.AREA_USR = b.AREA_USR" 

    sql.execute "DROP TABLE IF EXISTS USR_TYPO"
    sql.execute "CREATE TABLE USR_TYPO AS SELECT a.PK, a.THE_GEOM,a.MAJO,b.SECOND "+
        "FROM USR_TYPO_WITH_NULL_VALUE a JOIN USR_TYPO_WITH_NULL_VALUE b ON a.PK = b.PK AND a.MAJO IS NOT NULL "+
        "MINUS SELECT a.PK,a.THE_GEOM,a.MAJO,a.SECOND FROM USR_TYPO_WITH_NULL_VALUE a "+
        "WHERE (SELECT COUNT(*) FROM USR_TYPO_WITH_NULL_VALUE b WHERE  a.PK = b.PK) = 2 AND SECOND IS NULL"
 
    logger.info "Clean temporary tables"  
    sql.execute "DROP TABLE IF EXISTS SUM_AREA_USR"
    sql.execute "DROP TABLE IF EXISTS TOP2_AREA"  
    sql.execute "DROP TABLE IF EXISTS USR_TYPO_WITH_NULL_VALUE"


    literalOutput = "The classification has been done. The tables USR_TYPO and BUILDING_TYPO have been created correctly" 
}


/** String output of the process. */
@LiteralDataOutput(
    title="Output message",
    resume="The output message")
String literalOutput

