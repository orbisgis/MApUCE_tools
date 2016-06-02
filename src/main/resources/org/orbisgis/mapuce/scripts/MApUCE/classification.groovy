import org.orbisgis.wpsgroovyapi.input.*
import org.orbisgis.wpsgroovyapi.output.*
import org.orbisgis.wpsgroovyapi.process.*
import javax.swing.JOptionPane
import org.orbisgis.mapuce.randomForest.*
import java.sql.ResultSet
import java.sql.Statement



/**
 * This process is used to join the tables (USR, BLOCK and BUILDINGS) to match with the model of Alexandre.
 * When this join is done the process make the classification.
 * The user has to specify (mandatory):
 *  - Each table (USR,BLOCK,BUILDINGS)
 *  - An ID (primary key of buildings)
 *  - An Output Table where are store the result
 *
 * @return The table whith an ID and the class of each buildings
 *
 * @author Melvin Le Gall
 */
@Process(title = "Join USR,BLOCK,BUILDINGS and classification",
        resume = "Join USR,BLOCK,BUILDINGS to match with the model of Alexandre necessary for the classification,After the classification run for return a table of result",
        keywords = "Vector,MAPuCE")
def processing() {


  /**
  * First join between Block and Buildings
  */
  logger.warn "Make join betwenn Buildings and Block"
  sql.execute "DROP TABLE IF EXISTS BLOCK_BATI"
  sql.execute "CREATE TABLE BLOCK_BATI AS SELECT a.PK_USR,b.PK_BLOCK AS B_PK,a.PK AS I_PK,a.HAUTEUR_ORIGIN AS i_H_Origin,a.INSEE_INDIVIDUS AS i_INHAB,a.HAUTEUR AS i_H,a.NB_NIV AS i_LEVELS,a.AREA AS i_AREA,a.FLOOR_AREA AS i_FLOOR,a.VOL AS i_VOL,a.COMPACITY_R AS i_COMP_B,a.COMPACITY_N AS i_COMP_N,a.COMPACTNESS AS i_COMP,a.FORM_FACTOR AS i_FORM,a.CONCAVITY AS i_CONC, a.MAIN_DIR_DEG AS i_DIR,a.B_FLOOR_LONG AS i_PERI,a.B_WALL_AREA AS i_WALL_A,a.P_WALL_LONG AS i_PWALL_L,a.P_WALL_AREA AS i_PWALL_A,a.NB_NEIGHBOR AS i_Nb_NEI,a.FREE_P_WALL_LONG AS i_FWALL_L,a.FREE_EXT_AREA AS i_FREE_EXT_AREA,a.CONTIGUITY AS i_CONTIG,a.P_VOL_RATIO AS i_PASSIV_VOL,a.FRACTAL_DIM AS i_FRACTAL,a.MIN_DIST AS i_DIST_MIN,a.MEAN_DIST AS i_DIST_MEAN,a.MAX_DIST AS i_DIST_MAX,a.STD_DIST AS i_DIST_STD,a.NUM_POINTS AS i_Nb_POINTS,a.L_TOT AS i_L_TOT,a.L_CVX AS i_L_CVX,a.L_3M AS i_L_3M,b.AREA AS b_AREA,b.FLOOR_AREA AS b_FLOOR,b.VOL AS b_VOL,b.H_MEAN  AS b_H_MEAN,b.H_STD AS b_H_STD,b.COMPACITY AS b_COMP_N,b.HOLES_PERCENT AS b_HOLES_P,b.MAIN_DIR_DEG AS b_DIR "+
  "FROM BUILDING_INDICATORS a JOIN BLOCK_INDICATORS b"+
  " ON a.PK_USR = b.PK_USR AND a.PK_BLOCK = b.PK_BLOCK"

  /**
  * Last join between block_bati and usr
  */
  logger.warn "create the final table use to classify"
  sql.execute "DROP TABLE IF EXISTS BLOCK_BATI_USR"
  sql.execute "CREATE TABLE BLOCK_BATI_USR AS SELECT a.I_PK,a.i_H_ORIGIN,a.i_INHAB,a.i_H,a.i_LEVELS,a.i_AREA,a.i_FLOOR,a.i_VOL,a.i_COMP_B,a.i_COMP_N,a.i_COMP,a.i_FORM,a.i_CONC,a.i_DIR,a.i_PERI,a.i_WALL_A,a.i_PWALL_L,a.i_PWALL_A,a.i_Nb_NEI,a.i_FWALL_L,a.i_FREE_EXT_AREA,a.i_CONTIG,a.i_PASSIV_VOL,a.i_FRACTAL,a.i_DIST_MIN,a.i_DIST_MEAN,a.i_DIST_MAX,a.i_DIST_STD,a.i_Nb_POINTS,a.i_L_TOT,a.i_L_CVX,a.i_L_3M,a.b_AREA,a.b_FLOOR,a.b_VOL,a.b_H_MEAN,a.b_H_STD,a.b_COMP_N,a.b_HOLES_P,a.b_DIR,u.VEGETATION_SURFACE AS u_VEG_A,u.ROUTE_SURFACE AS u_ROAD_A,u.ROUTE_LONGUEUR AS u_ROAD_L,u.TROTTOIR_LONGUEUR AS u_SIDEWALK_L,u.INSEE_INDIVIDUS AS u_INHAB,u.INSEE_MENAGES AS u_HOUSE,u.INSEE_MEN_COLL AS u_COL_HOUSE,u.INSEE_MEN_SURF AS u_HOUSE_A,u.insee_surface_collectif AS u_COL_HOUSE_A,u.FLOOR AS u_FLOOR,u.FLOOR_RATIO AS u_COS,u.COMPAC_MEAN_NW AS u_COMP_NWMEAN,u.COMPAC_MEAN_W AS u_COMP_WMEAN,u.CONTIG_MEAN AS u_CONTIG_MEAN,u.CONTIG_STD AS u_CONTIG_STD,u.MAIN_DIR_STD AS u_DIR_STD,u.H_MEAN AS u_H_MEAN,u.H_STD AS u_H_STD,u.P_VOL_RATIO_MEAN AS u_PASSIV_VOL_MEAN,u.B_AREA AS u_AREA,u.B_VOL AS u_VOL,u.B_VOL_M AS u_VOL_MEAN,u.BUILD_NUMB AS u_Nb_BUILD,u.MIN_M_DIST AS u_DIST_MIN_MEAN,u.MEAN_M_DIST AS u_DIST_MEAN_MEAN,u.MEAN_STD_DIST AS u_DIST_MEAN_STD,u.B_STD_H_MEAN AS u_bH_STD_MEAN,u.B_M_NW_COMPACITY AS u_bCOMP_NWMEAN,u.B_M_W_COMPACITY AS u_bCOMP_WMEAN,u.B_STD_COMPACITY AS u_bCOMP_STD,u.DIST_TO_CENTER AS u_DIST_CENTER,u.BUILD_DENS AS u_BUILD_DENS,u.VEGET_DENS AS u_VEG_DENS,u.ROAD_DENS AS u_ROAD_DENS,u.EXT_ENV_AREA AS u_FWALL_A FROM BLOCK_BATI AS A JOIN USR_INDICATORS AS U ON a.PK_USR = u.PK"


  ClassifyData cla = new ClassifyData(model,sql.createConnection())
 
  Statement sta = sql.createConnection().createStatement()
  String req =  "SELECT * FROM BLOCK_BATI_USR" 
  ResultSet rs = sta.executeQuery(req) 

  //Transform ResultSet in Instances  
  logger.warn "Transform ResultSet in Instances"
  cla.resultSetToInstances(rs,"I_PK")

  //Make the classification and create the table TYPO_RESULT  
  logger.warn "Make the classification and create the table TYPO_RESULT"
  cla.classify("TYPO_RESULT")

  logger.warn "Clean temporary tables"  
  sql.execute "DROP TABLE IF EXISTS BLOCK_BATI"
  sql.execute "DROP TABLE IF EXISTS BLOCK_BATI_USR"

  logger.warn "Done"
}


@RawDataInput(
 title="Model",
        resume="The name of output table")
String model

/** String output of the process. */
@LiteralDataOutput(
        title="Output message",
        resume="The output message")
String literalOutput

