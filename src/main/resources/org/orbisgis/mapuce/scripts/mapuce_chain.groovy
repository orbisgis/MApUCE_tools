import org.orbisgis.mapuce.WpsScriptsPackage;
import org.orbisgis.wpsgroovyapi.input.*
import org.orbisgis.wpsgroovyapi.output.*
import org.orbisgis.wpsgroovyapi.process.*
import javax.swing.JOptionPane;
import javax.script.ScriptEngine;
import org.apache.commons.io.FileUtils


/**
 * This process is used to run the MAPuCE geoprocessing chain.
 * 
 *
 * @author Erwan Bocher
 */
@Process(title = "Complete geoprocessing chain",
        resume = "This script allows to chain all processes for a set of spatial units.<br>A spatial unit is a french commune area defined by a unique identifier called CODE_INSEE.<br>The user can select one of more spatial units based on a list of CODE_INSEE or a selection of unit area names.<br>If the user select a unit area a pre-process is done to return its corresponding list of CODE_INSEE.<br>For each spatial unit, 3 steps are executed : <ul> <li>1. Extract input data (USR, Buildings, Roads) from the Mapuce DataBase</li><li>2. Compute morphological indicators</li><li>3. Merge indicators into 3 tables : final_building_indicators,final_block_indicators,final_usr_indicators </li></ul><br>Note:<br> The imported data are stored into a remote database. Please contact info@orbigis.org to obtain an account. <br> The list of available communes must be already imported to get the list of available unit areas or CODE_INSEE. If not please execute the script to import all commune areas...",
        keywords = ["Vector","MAPuCE"])
def processing() {
if(!login.isEmpty()&& !password.isEmpty()){        
        codesInsee = prepareCodes(fieldCodes,  codesInsee);
        prepareFinalTables();
        engine = initChain();
        logger.warn "Number of selected areas : ${codesInsee.length}" 
        int i=1;
        for (code in codesInsee) {
            logger.warn "Start processing for area : ${code} -> Number ${i} on ${codesInsee.length}"  
            if(importData(code)){
            //Compute indicators
            computeIndicators(code);
            mergeIndicatorsIntoFinalTables();
            //Apply random forest classification
            applyRandomForest(engine)
            cleanTables();
            logger.warn "End processing for area : ${code}"               
            }
            else{
                logger.warn "Cannot import the data for the area : ${code}."   
            }
            i++;
        }
    
    //Create a typo legend table
    sql.execute "create table typo_label (typo varchar(5), label varchar)"
    sql.execute "insert into typo_label VALUES ('bgh','Bâtiment de grande hauteur')"
    sql.execute "insert into typo_label VALUES ('pcio','Pavillon continu sur îlot ouvert')"
    sql.execute "insert into typo_label VALUES ('pd' ,'Pavillon discontinu')"
    sql.execute "insert into typo_label VALUES ('local' , 'Local annexe')"
    sql.execute "insert into typo_label VALUES ('pcif' , 'Pavillon continu sur îlot fermé')"
    sql.execute "insert into typo_label VALUES ('icif', 'Immeuble continu sur îlot fermé')"
    sql.execute "insert into typo_label VALUES ('psc', 'Pavillon semi-continu')"
    sql.execute "insert into typo_label VALUES ('icio','Immeuble continu sur îlot ouvert')"
    sql.execute "insert into typo_label VALUES ('ba', 'Bâtiment d''activité')"
    sql.execute "insert into typo_label VALUES ('id' , 'Immeuble discontinu')"
   
    literalOutput = "The chain has been executed..."
}

}

/**
 * Returns an array with all insee codes even if the selected field is for urban areas.
 *
 **/
def prepareCodes(String[] fieldCodes, String[] codesInsee ){
    if (fieldCodes[0].equalsIgnoreCase("unite_urbaine")){
        def codesUU = []
        sql.eachRow("select code_insee from COMMUNES_MAPUCE where unite_urbaine in(${codesInsee.join(',')});"){row ->
            codesUU.add(row.code_insee)            
        }
        codesInsee = codesUU as String[]
    }
    return codesInsee;
}

/**
 * Init the Renjin engine to apply the R code
 * */
def initChain(){
    def modelName = "mapuce-rf-2.2.RData";

    logger.warn "Download the MAPuCE model - $modelName - used to classify the buildings."    
    

    //Do not download the file is already exist
    File file = new File(System.getProperty("user.home") + "/mapuce/"+ modelName );
    
    if(!file.exists()){
        FileUtils.copyURLToFile(new URL("https://github.com/orbisgis/MApUCE_tools/raw/master/model/$modelName"), file)   
    }
    
    engine = rEngine.getScriptEngine();
    engine.put("con", rEngine.getConnectionRObject(sql.getDataSource().getConnection())); 
    engine.put("model_path", file.getAbsolutePath()) 
    
    return engine;
    
}
/**
 * Prepare the 3 tables to store all results
 * */
def prepareFinalTables(){
    sql.execute "drop table if exists final_usr_indicators, final_block_indicators,final_building_indicators,FINAL_BUILDING_TYPO, FINAL_USR_TYPO, typo_label;"     
    sql.execute "DROP TABLE IF EXISTS BUILDING_INDICATORS, USR_INDICATORS, BLOCK_INDICATORS"
    sql.execute "DROP SCHEMA IF EXISTS DATA_WORK"
    sql.execute "CREATE TABLE final_building_indicators (PK_BUILDING INTEGER,PK_USR INTEGER, ID_ZONE INTEGER,  THE_GEOM POLYGON, HAUTEUR_ORIGIN  double precision,  NB_NIV  double precision, HAUTEUR  double precision, AREA  double precision, PERIMETER  double precision, INSEE_INDIVIDUS  double precision, FLOOR_AREA  double precision, VOL  double precision, COMPACITY_R  double precision, COMPACITY_N   double precision, COMPACTNESS  double precision, FORM_FACTOR  double precision, CONCAVITY  double precision, MAIN_DIR_DEG  double precision, B_FLOOR_LONG  double precision, B_WALL_AREA  double precision, P_WALL_LONG  double precision, P_WALL_AREA  double precision, NB_NEIGHBOR  double precision, FREE_P_WALL_LONG double precision, FREE_EXT_AREA double precision, CONTIGUITY double precision, P_VOL_RATIO double precision, FRACTAL_DIM double precision, MIN_DIST double precision, MEAN_DIST double precision, MAX_DIST double precision, STD_DIST double precision, NUM_POINTS integer, L_TOT double precision, L_CVX double precision, L_3M double precision, L_RATIO double precision, L_RATIO_CVX double precision, PK_BLOCK_ZONE INTEGER, THEME varchar, PAI_BDTOPO varchar, PAI_NATURE varchar);"
    sql.execute "CREATE TABLE final_block_indicators (PK_BLOCK_ZONE INTEGER, PK_USR INTEGER,THE_GEOM POLYGON,  AREA double precision, FLOOR_AREA double precision, VOL double precision, H_MEAN double precision, H_STD double precision, COMPACITY double precision, HOLES_AREA double precision, HOLES_PERCENT double precision, MAIN_DIR_DEG double precision );"
    sql.execute "CREATE TABLE final_usr_indicators (PK_USR INTEGER, ID_ZONE integer NOT NULL,THE_GEOM MULTIPOLYGON,  insee_individus double precision,insee_menages double precision ,insee_men_coll double precision ,insee_men_surf double precision ,insee_surface_collectif double precision,VEGETATION_SURFACE double precision, ROUTE_SURFACE double precision,route_longueur double precision, trottoir_longueur double precision,   floor double precision,   floor_ratio double precision,   compac_mean_nw double precision,   compac_mean_w double precision,   contig_mean double precision,   contig_std double precision,   main_dir_std double precision,   h_mean double precision,   h_std double precision,   p_vol_ratio_mean double precision,   b_area double precision,   b_vol double precision,   b_vol_m double precision,   build_numb integer,   min_m_dist double precision,   mean_m_dist double precision,   mean_std_dist double precision,   b_holes_area_mean double precision,   b_std_h_mean double precision,   b_m_nw_compacity double precision,   b_m_w_compacity double precision,   b_std_compacity double precision,   dist_to_center double precision,   build_dens double precision,   hydro_dens double precision,   veget_dens double precision,   road_dens double precision,   ext_env_area double precision, dcomiris varchar )"
    
   
    sql.execute "create table FINAL_BUILDING_TYPO(the_geom geometry, pk_building integer,pk_usr integer,id_zone integer, typo varchar)"
    sql.execute "create table FINAL_USR_TYPO(the_geom geometry, pk_usr integer, id_zone integer,ba float,bgh float,icif float, icio float, id float, local float, pcif float, pcio float, pd float, psc float ,typo_maj varchar, typo_second varchar)"
     
    /**
    * Indicators definition
    **/
    sql.execute "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.the_geom IS ' External border of the union of a set of touching geometry buildings';"
    sql.execute "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.PK_BLOCK_ZONE IS 'Unique identifier for a block geometry for the current commune';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.PK_USR IS 'Unique identifier of the usr';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.area IS 'Area of the block';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.floor_area IS 'Sum of building the floor areas in the block';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.vol IS 'Sum of the building volumes in a block';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.h_mean IS 'Buildings’s mean height in each block';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.h_std IS 'Buildings’s Standard Deviation height in each block';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.compacity IS 'The block’s compacity is defined as the sum of the building’s external surfaces divided by the sum of building’s volume power two-third';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.holes_area IS ' Sum of holes’s area (courtyard) in a block';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.holes_percent IS 'Compute the ratio (percent) of courtyard area in a block of buildings';"
    sql.execute  "COMMENT ON COLUMN FINAL_BLOCK_INDICATORS.main_dir_deg IS 'The main direction corresponds to the direction (in degree) given by the longest side of the geometry’s minimum rectangle. The north is equal to 0°. Values are clockwise, so East = 90°.the value is between 0 and 180°(e.g 355° becomes 175°).';"
    
    
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.the_geom IS 'Geometry of the building. This geometry is normalized to avoid topological errors';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.PK_BUILDING IS 'Unique identifier for a building';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.PK_BLOCK_ZONE IS 'Unique identifier for a block geometry for the current commune';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.perimeter IS 'Building perimeter';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.INSEE_INDIVIDUS IS 'Number of inhabitants derived from intersection of INSEE 200m gridded cells, taking into account the pai_nature (must be null), and the developped area (= area(building) * nb_niv)';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.hauteur IS ' Heigth of the building';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.hauteur_origin IS ' Approximate heigth of the building when the value hauteur is null';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.nb_niv IS 'Number of levels for a building based on the field hauteur';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.id_zone IS 'Unique identifier of a commune';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.pk_usr IS 'Unique identifier of the USR';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.area IS 'Area of the building';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.floor_area IS 'Sum of the building’s area for each level';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.vol IS 'Product of the area and the height';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.compacity_r IS 'Sum of external surfaces divided by the building’s volume power two-third';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.compacity_n IS 'Sum of free external surfaces divided by the building’s volume power two-third.';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.compactness IS 'The compactness ratio is defined as the ratio between the polygon’s length and the perimeter of a circle with the same area (Gravelius’s definition).';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.form_factor is 'Ratio between the polygon’s area and the square of the building’s perimeter.';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.concavity IS 'Ratio between the geometry’s area and its convex hull’s area';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.main_dir_deg IS 'The main direction corresponds to the direction (in degree) given by the longest side of the geometry’s minimum rectangle.The north is equal to 0°. Values are clockwise, so East = 90°. This value is ”modulo pi” expressed → the value is between 0 and 180° (e.g 355° becomes 175°).';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.b_floor_long IS 'Building perimeter';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.b_wall_area IS 'Total area of building’s walls (area of facade, including holes)';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.p_wall_long IS 'Total length of party walls';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.p_wall_area IS 'Total area of common walls (based on common hight of buildings)';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.nb_neighbor IS 'Number of party walls';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.free_p_wall_long IS 'Total length of free facades (= perimeter - total length of party walls)';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.free_ext_area IS 'Total area of free external facades';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.contiguity IS 'Ratio of total area of party walls divided by building’s facade area';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.p_vol_ratio IS 'Passiv volume ratio';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.fractal_dim IS 'The fractal dimension of the geometry is defined as 2log(Perimeter)/log(area) ';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.min_dist IS 'Minimum distance between one building and all the others that are in the USR';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.mean_dist IS 'Mean distance between one building and all the others that are in the USR';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.max_dist IS 'Maximum distance between one building and all the others that are in the USR';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.std_dist IS 'Standard deviation distance between one building and all the others that are in the USR';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.num_points IS 'Number of distinct points of the exterior ring of the building';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.l_tot IS 'Building’s perimeter of the exterior ring';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.l_cvx IS 'Perimeter of the convex hull of the building';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.l_3m IS 'Length of walls that are less than 3 meters from a road.';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.l_ratio IS 'Ratio between L_TOT and L_3M';"
    sql.execute "COMMENT ON COLUMN FINAL_BUILDING_INDICATORS.l_ratio_cvx IS 'Ratio between L_3M and L_CVX';"
    
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.the_geom IS 'Geometry of the USR.';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.INSEE_INDIVIDUS IS 'Number of inhabitants computed by sum of INSEE grid cells intersecting buildings of the USR, proportionaly to their floor area (nb_niv * area(building)) and only if the pai_nature of the building is null (which means residential a priori).)';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.INSEE_MENAGES IS 'Number of households having a permanent living computed by sum of INSEE grid cells intersecting buildings of the USR, proportionaly to their floor area (nb_niv * area(building)), and only if the pai_nature of the building is null (which means residential a priori).';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.INSEE_MEN_COLL IS 'Number of households living in collective housing computed by sum of INSEE grid cells intersecting buildings of the USR, proportionaly to their floor area (nb_niv * area(building)), and only if the pai_nature of the building is null (which means residential a priori).';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.INSEE_MEN_SURF IS 'Cumulated area of housings for households having a permanent living computed in square meter, by summing share of INSEE grid cells intersecting buildings of the islets, proportionaly to their developed area (nb_niv * area(building)), and only if the pai_nature of the building is null (which means residential a priori).';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.INSEE_SURFACE_COLLECTIF IS 'Estimation of collective housing from INSEE indicators: (=insee_men_coll/insee_menages*insee_men_surf)';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.VEGETATION_SURFACE IS 'Area of vegetation intersecting the USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.ROUTE_SURFACE IS 'Area of roads intersecting the USR. Computing is using the lenght of road segments intersecting the USR, and length of a clip of each segment with islet frontiers is multiplicated by a buffer having the road width divided by 2. Nota: roads with fictif to true have automatically a width of 0, and 65% of secondary roads with importance=5 and fictif=false have a null width. ';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.ROUTE_LONGUEUR IS 'Length of roads intersecting the USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.TROTTOIR_LONGUEUR IS 'Perimeter of the included USR made of union of contiguous parcels.';"    
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.floor IS 'Sum of each building’s floor area';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.floor_ratio IS 'Ratio between the total floor area and the USR area';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.compac_mean_nw IS 'Non weighted buildings’s mean compacity in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.compac_mean_w IS 'Weighted buildings’s mean compacity in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.contig_mean IS 'Buildings’s mean contiguity in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.contig_std IS 'Buildings’s standard deviation contiguity in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.main_dir_std IS 'Buildings’s standard deviation main direction in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.h_mean IS 'Buildings’s mean height in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.h_std IS 'Buildings’s standard deviation height in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.p_vol_ratio_mean IS 'Buildings’s mean passiv volume ratio in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.b_area IS 'Total building’s area in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.b_vol IS 'Total building’s volume in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.b_vol_m IS 'Buildings’s mean volume in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.build_numb IS 'Buildings’s number in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.min_m_dist IS 'Mean value of the minimum distance between buildings in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.mean_m_dist IS 'Mean value of the mean distance between buildings in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.mean_std_dist IS 'Standard deviation of the mean distance between buildings in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.b_holes_area_mean IS 'Blocks’s mean courtyard ratio in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.b_std_h_mean IS 'Blocks’s mean standard deviation height in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.b_m_nw_compacity IS 'Block’s non weigthed mean compacity in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.b_m_w_compacity IS 'Block’s weigthed mean compacity in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.b_std_compacity IS 'Blocks’s standard deviation compacity in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.dist_to_center IS 'Distance between an USR (centroid) and its commune (centroid)';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.build_dens IS 'Buildings’s area density value';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.hydro_dens IS 'Hydrographic’s area density value';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.veget_dens IS 'Vegetation’s area density value';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.road_dens IS 'Road’s area density value';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.ext_env_area IS 'Total building’s external surface in an USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.id_zone IS 'Unique identifier of a commune';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.PK_USR IS 'Unique identifier of the USR';"
    sql.execute "COMMENT ON COLUMN FINAL_USR_INDICATORS.dcomiris IS 'Unique identifier of the IRIS';"
    
}

/**
 * Method to merge indicators into the final tables and remove intermediate tables
 * */
def mergeIndicatorsIntoFinalTables(){
    sql.execute "INSERT INTO FINAL_BUILDING_INDICATORS (SELECT * FROM BUILDING_INDICATORS);"
    sql.execute "INSERT INTO FINAL_BLOCK_INDICATORS (SELECT * FROM BLOCK_INDICATORS);"
    sql.execute "INSERT INTO FINAL_USR_INDICATORS (SELECT * FROM USR_INDICATORS);"
  }
  
def cleanTables(){
      sql.execute "DROP TABLE IF EXISTS USR_INDICATORS, BUILDING_INDICATORS,BLOCK_INDICATORS, COMMUNE_MAPUCE, USR_MAPUCE, ROADS_MAPUCE,BUILDINGS_MAPUCE ;"
      sql.execute "DROP TABLE IF EXISTS TYPO_BUILDINGS_MAPUCE,TYPO_USR_MAPUCE ;"
}

/**
 * This method is used to import all data from the remote database 
 */
 def importData(String code){   
   logger.warn "Importing the USR"
   sql.execute "DROP TABLE IF EXISTS USR_TEMP"
            def schemaFromRemoteDB = "lienss"	
            def tableFromRemoteDB = "(SELECT * FROM lienss.usr WHERE CODE_INSEE=''"+code+"'')"
    def query = "CREATE LINKED TABLE USR_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
            query+=" '"+ login+"',"
            query+="'"+password+"', '"+schemaFromRemoteDB+"', "
            query+= "'"+tableFromRemoteDB+"')";
            sql.execute query
            sql.execute "drop table if exists USR_MAPUCE"
            sql.execute "CREATE TABLE USR_MAPUCE(PK integer, THE_GEOM geometry, ID_ZONE integer NOT NULL, VEGETATION_SURFACE double, ROUTE_SURFACE double, HYDRO_SURFACE double, USR_AREA double, insee_individus double,insee_menages double ,insee_men_coll double ,insee_men_surf double ,insee_surface_collectif double,route_longueur double, trottoir_longueur double) AS SELECT PK, THE_GEOM, IDZONE as ID_ZONE, VEGETATION_SURFACE, ROUTE_SURFACE, HYDRO_SURFACE, ST_AREA(THE_GEOM) as USR_AREA , insee_individus,insee_menages,insee_men_coll,insee_men_surf ,insee_surface_collectif ,route_longueur , trottoir_longueur  FROM USR_TEMP"
            sql.execute "CREATE SPATIAL INDEX ON USR_MAPUCE(THE_GEOM);"
            sql.execute "CREATE INDEX ON USR_MAPUCE(PK);"
            sql.execute "DROP TABLE USR_TEMP"

            logger.warn "Importing the buildings"
    sql.execute "DROP TABLE IF EXISTS BUILDINGS_TEMP"
            tableFromRemoteDB = "(SELECT a.IDZONE, a.THE_GEOM, a.HAUTEUR, a.IDILOT as PK_USR, a.PK, a.NB_NIV, a.HAUTEUR_CORRIGEE, a.INSEE_INDIVIDUS, a.THEME,a.PAI_BDTOPO,a.PAI_NATURE, b.CODE_INSEE  FROM lienss.BATI_TOPO a, lienss.ZONE_ETUDE b WHERE a.IDZONE = b.OGC_FID and b.CODE_INSEE=''"+code+"'' and a.IDILOT IS NOT NULL and  ST_NUMGEOMETRIES(a.the_geom)=1 and ST_ISEMPTY(a.the_geom)=false)"
    query = "CREATE LINKED TABLE BUILDINGS_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
            query+=" '"+ login+"',"
            query+="'"+password+"', '"+schemaFromRemoteDB+"', "
            query+= "'"+tableFromRemoteDB+"')";
            sql.execute query
    sql.execute "drop table if exists BUILDINGS_MAPUCE"
    sql.execute "CREATE TABLE BUILDINGS_MAPUCE (THE_GEOM geometry, HAUTEUR_ORIGIN double, ID_ZONE integer, PK_USR integer, PK integer PRIMARY KEY, NB_NIV integer, HAUTEUR double, AREA double, PERIMETER double, L_CVX double, INSEE_INDIVIDUS double, THEME varchar, PAI_BDTOPO varchar, PAI_NATURE varchar ) AS SELECT  ST_NORMALIZE (THE_GEOM) as THE_GEOM, (HAUTEUR :: double) as HAUTEUR_ORIGIN, IDZONE as ID_ZONE, PK_USR, PK :: integer, NB_NIV, (HAUTEUR_CORRIGEE :: double) as HAUTEUR, ST_AREA(THE_GEOM) as AREA, ST_PERIMETER(THE_GEOM) as PERIMETER , ROUND(ST_PERIMETER(ST_CONVEXHULL(THE_GEOM)),3), INSEE_INDIVIDUS, THEME , PAI_BDTOPO, PAI_NATURE FROM BUILDINGS_TEMP "

    sql.execute "UPDATE BUILDINGS_MAPUCE SET HAUTEUR=3 WHERE HAUTEUR is null OR HAUTEUR=0"
    sql.execute "UPDATE BUILDINGS_MAPUCE SET NB_NIV=1 WHERE NB_NIV=0"
    sql.execute "CREATE SPATIAL INDEX ON BUILDINGS_MAPUCE(THE_GEOM)"
    sql.execute "CREATE INDEX ON BUILDINGS_MAPUCE(PK_USR)"
    sql.execute "DROP TABLE  BUILDINGS_TEMP"
    
    //A check due to some errors in the input data. eg. Buildings with null usr id.
    //Do not import the roads if there are no buildings
    
    def cnt = sql.rows('SELECT  count(*) as cnt  FROM BUILDINGS_MAPUCE ;').cnt[0]    
    if(cnt>0){
    logger.warn "Importing the roads"
    sql.execute "DROP TABLE IF EXISTS ROADS_TEMP"
    schemaFromRemoteDB = "ign_bd_topo_2014"
    tableFromRemoteDB = "(SELECT * FROM ign_bd_topo_2014.ROUTE WHERE INSEECOM_D=''"+code+"'' OR INSEECOM_G=''"+code+"'')"
    query = "CREATE LINKED TABLE ROADS_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
            query+=" '"+ login+"',"
            query+="'"+password+"', '"+schemaFromRemoteDB+"', "
            query+= "'"+tableFromRemoteDB+"')";
            sql.execute query
    sql.execute "drop table if exists ROADS_MAPUCE"
    sql.execute "CREATE TABLE ROADS_MAPUCE (PK integer, THE_GEOM geometry, LARGEUR double, INSEECOM_G varchar(5), INSEECOM_D varchar(5))AS SELECT PK , THE_GEOM, LARGEUR, INSEECOM_G, INSEECOM_D FROM ROADS_TEMP"
    sql.execute "CREATE SPATIAL INDEX ON ROADS_MAPUCE(THE_GEOM)"
    sql.execute "DROP TABLE IF EXISTS ROADS_TEMP"

    logger.warn "Importing the geometry of the spatial unit"    
    sql.execute "DROP TABLE IF EXISTS COMMUNE_MAPUCE_TEMP, COMMUNE_MAPUCE"
    schemaFromRemoteDB = "lienss"
    tableFromRemoteDB = "(SELECT DISTINCT ON (CODE_INSEE) CODE_INSEE, unite_urbaine, the_geom FROM lienss.zone_etude WHERE CODE_INSEE=''"+code+"'')"
    query = "CREATE LINKED TABLE COMMUNE_MAPUCE_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";    
    sql.execute query
    sql.execute "CREATE TABLE COMMUNE_MAPUCE AS SELECT * FROM COMMUNE_MAPUCE_TEMP"    	
    sql.execute "DROP TABLE IF EXISTS COMMUNE_MAPUCE_TEMP"
    
    logger.warn "Importing the IRIS geometries"
    sql.execute "DROP TABLE IF EXISTS IRIS_MAPUCE_TEMP, IRIS_MAPUCE"
    schemaFromRemoteDB = "lra"
    tableFromRemoteDB = "(SELECT * FROM lra.iris_date_fm_3 where depcom=''"+code+"'')"
    query = "CREATE LINKED TABLE IRIS_MAPUCE_TEMP ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";  
    sql.execute query
    sql.execute "CREATE TABLE IRIS_MAPUCE AS SELECT * FROM IRIS_MAPUCE_TEMP"        
    sql.execute "DROP TABLE IF EXISTS IRIS_MAPUCE_TEMP"
    
    }
    else{
        return false;
    }
    return true;
    
}


/**
 * This method is used to compute all indicators 
 */
 def computeIndicators(String code){  
     
    sql.execute "CREATE SCHEMA DATA_WORK"

    /**
    * Compute the building indicators
    **/

    logger.warn "Compute area volume"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_AREA_VOL (PK integer primary key, FLOOR_AREA double precision, VOL double precision) AS SELECT PK, (AREA * NB_NIV) AS FLOOR_AREA , (AREA * HAUTEUR) AS VOL FROM BUILDINGS_MAPUCE"

    logger.warn "Compute form factor"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_FORM_FACTOR (PK integer primary key , FORM_FACTOR double precision) AS SELECT PK , AREA / POWER(PERIMETER ,2) AS FORM_FACTOR FROM BUILDINGS_MAPUCE"

    logger.warn "Compute concavity"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_CONCAVITY (PK integer primary key, CONCAVITY double precision) AS SELECT PK, (AREA / ST_AREA(ST_CONVEXHULL(THE_GEOM))) AS CONCAVITY FROM BUILDINGS_MAPUCE"

    logger.warn "Compute contiguity"


    sql.execute "CREATE TABLE DATA_WORK.CONTIGUITY_INTERSECTION AS SELECT a.PK as PK_D, a.HAUTEUR as H_D, b.PK as PK_G, b.HAUTEUR as H_G, ST_AREA(a.THE_GEOM) as B_FLOOR_AREA, a.PERIMETER as B_FLOOR_LONG, (a.PERIMETER * a.HAUTEUR) as B_WALL_AREA, ST_INTERSECTION(a.THE_GEOM, b.THE_GEOM) as THE_GEOM FROM BUILDINGS_MAPUCE a, BUILDINGS_MAPUCE b WHERE a.PK <> b.PK AND a.THE_GEOM && b.THE_GEOM AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM)"


    sql.execute "CREATE TABLE DATA_WORK.BUILD_CONTIGUITY_GEOM AS SELECT PK_D as PK, B_FLOOR_AREA, B_FLOOR_LONG, B_WALL_AREA, LEAST(H_D, H_G) as H_MIN, ST_LENGTH(ST_COLLECTIONEXTRACT(THE_GEOM,2)) as P_WALL_LONG, (LEAST(H_D, H_G) * ST_LENGTH(ST_COLLECTIONEXTRACT(THE_GEOM,2))) as P_WALL_AREA FROM DATA_WORK.CONTIGUITY_INTERSECTION"
    sql.execute "CREATE INDEX ON DATA_WORK.BUILD_CONTIGUITY_GEOM(PK)"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_CONTIGUITY (PK integer primary key, B_FLOOR_AREA double precision, B_FLOOR_LONG double precision, B_WALL_AREA double precision, P_WALL_LONG double precision, P_WALL_AREA double precision, NB_NEIGHBOR integer) AS SELECT PK, B_FLOOR_AREA, B_FLOOR_LONG, B_WALL_AREA, SUM(P_WALL_LONG) as P_WALL_LONG, SUM(P_WALL_AREA) as P_WALL_AREA, COUNT(*) as NB_NEIGHBOR FROM DATA_WORK.BUILD_CONTIGUITY_GEOM GROUP BY PK, B_FLOOR_AREA, B_FLOOR_LONG, B_WALL_AREA"

    sql.execute "ALTER TABLE DATA_WORK.BUILD_CONTIGUITY ADD COLUMN FREE_P_WALL_LONG double precision as B_FLOOR_LONG-P_WALL_LONG"
    sql.execute "ALTER TABLE DATA_WORK.BUILD_CONTIGUITY ADD COLUMN FREE_EXT_AREA double precision as (B_WALL_AREA - P_WALL_AREA + B_FLOOR_AREA)"
    sql.execute "ALTER TABLE DATA_WORK.BUILD_CONTIGUITY ADD COLUMN CONTIGUITY double precision as CASEWHEN(B_WALL_AREA>0, P_WALL_AREA/B_WALL_AREA, null)"

    logger.warn "Compute compacity";

    sql.execute "CREATE TABLE DATA_WORK.BUILD_COMPACITY_TMP AS SELECT a.PK, a.THE_GEOM, a.HAUTEUR, b.FREE_EXT_AREA FROM BUILDINGS_MAPUCE a LEFT JOIN DATA_WORK.BUILD_CONTIGUITY b ON a.PK=b.PK"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_COMPACITY (PK integer primary key, COMPACITY_R double precision , COMPACITY_N double precision) AS SELECT PK, (((ST_PERIMETER(THE_GEOM) * HAUTEUR) + ST_AREA(THE_GEOM)) / (POWER(ST_AREA(THE_GEOM) * HAUTEUR, (2./3.)))) as COMPACITY_R, (FREE_EXT_AREA/(POWER(ST_AREA(THE_GEOM) * HAUTEUR, (2./3.)))) as COMPACITY_N FROM DATA_WORK.BUILD_COMPACITY_TMP WHERE ST_AREA(THE_GEOM)!=0 AND HAUTEUR !=0"

    // If a building has no neighbors, so the net compacity is equal to the raw compacity
    sql.execute "UPDATE DATA_WORK.BUILD_COMPACITY SET COMPACITY_N = COMPACITY_R WHERE COMPACITY_N is null"

    logger.warn "Compute compactness"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_COMPACTNESS (PK integer primary key, COMPACTNESS double precision) AS SELECT PK, (PERIMETER/(2 * SQRT(PI() * AREA))) AS COMPACTNESS FROM BUILDINGS_MAPUCE"

    logger.warn "Compute main direction"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_MAIN_DIR (PK integer primary key, MAIN_DIR_DEG double precision) AS SELECT PK, MOD(CASEWHEN(ST_LENGTH(ST_MINIMUMDIAMETER(THE_GEOM))<0.1, DEGREES(ST_AZIMUTH(ST_STARTPOINT(THE_GEOM), ST_ENDPOINT(THE_GEOM))), DEGREES(ST_AZIMUTH(ST_STARTPOINT(ST_ROTATE(ST_MINIMUMDIAMETER(THE_GEOM),pi()/2)), ST_ENDPOINT(ST_ROTATE(ST_MINIMUMDIAMETER(THE_GEOM),pi()/2))))),180) as ROADS_MIN_DIR FROM BUILDINGS_MAPUCE"

    logger.warn "Compute passive volume ratio"

    // Initialisation of parameters
    // Tolerance distance (in meters)
    def EPSILON = 0.005
    // Size of the buffer (in meters)
    def BUFFER_IN = 6 + EPSILON

    sql.execute "CREATE TABLE DATA_WORK.merged_buildings(PK integer primary key, THE_GEOM geometry) AS SELECT ba.PK, ST_UNION(ST_BUFFER(ba.the_geom,"+EPSILON+", 'join=mitre'), ST_UNION(ST_ACCUM(ST_BUFFER(bb.the_geom,"+EPSILON+", 'join=mitre')))) geom FROM BUILDINGS_MAPUCE ba, BUILDINGS_MAPUCE bb WHERE ba.the_geom && ST_EXPAND(bb.the_geom,1,1) AND ba.PK <> bb.PK AND ST_INTERSECTS(ST_BUFFER(ba.the_geom,"+EPSILON+", 'join=mitre'), bb.the_geom) GROUP BY ba.PK, ba.the_geom"

    sql.execute "CREATE TABLE DATA_WORK.in_buffer(PK integer primary key, THE_GEOM geometry) AS SELECT ba.pk, ST_INTERSECTION(ST_BUFFER(ba.the_geom,2 * "+EPSILON+", 'join=mitre') ,ST_BUFFER(ST_SIDEBUFFER(CASEWHEN(mb.PK is null,ST_TOMULTISEGMENTS(ba.the_geom),ST_INTERSECTION(ST_BUFFER(ba.the_geom,10 * "+EPSILON+", 'join=mitre') ,ST_UNION(ST_TOMULTISEGMENTS(ST_HOLES(mb.the_geom)), ST_TOMULTISEGMENTS(ST_EXTERIORRING(mb.the_geom))))), -"+BUFFER_IN+"),"+EPSILON+", 'join=mitre')) the_geom FROM BUILDINGS_MAPUCE BA LEFT JOIN DATA_WORK.merged_buildings mb ON ba.PK = mb.PK"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_P_VOL_RATIO (PK integer PRIMARY KEY, THE_GEOM geometry, P_VOL_RATIO double precision) AS SELECT a.PK, b.THE_GEOM, ROUND(LEAST((ST_AREA(b.THE_GEOM)/AREA),1),2) as P_VOL_RATIO FROM BUILDINGS_MAPUCE a, DATA_WORK.in_buffer b WHERE a.PK=b.PK"

    logger.warn "Compute fractal dimension"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_FRACTAL (PK integer PRIMARY KEY, FRACTAL_DIM double precision) AS SELECT PK, (2 * LOG(PERIMETER)) / LOG(AREA) AS FRACTAL_DIM FROM BUILDINGS_MAPUCE WHERE AREA<>1"

    logger.warn "Compute the distance between buildings"

    sql.execute "CREATE TABLE DATA_WORK.DIST (PK integer PRIMARY KEY, MIN_DIST double precision,  MEAN_DIST double precision, MAX_DIST double precision, STD_DIST double precision, PK_USR integer)AS SELECT a.PK, MIN(ST_DISTANCE(a.THE_GEOM, b.THE_GEOM)) AS MIN_DIST, AVG(ST_DISTANCE(a.THE_GEOM, b.THE_GEOM)) AS MEAN_DIST, MAX(ST_DISTANCE(a.THE_GEOM, b.THE_GEOM)) AS MAX_DIST, STDDEV_POP(ST_DISTANCE(a.THE_GEOM, b.THE_GEOM)) AS STD_DIST,a.PK_USR FROM BUILDINGS_MAPUCE a, BUILDINGS_MAPUCE b WHERE a.PK<>b.PK AND a.PK_USR=b.PK_USR GROUP BY a.PK_USR, a.PK"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_DIST (PK integer PRIMARY KEY, MIN_DIST double precision,  MEAN_DIST double precision, MAX_DIST double precision, STD_DIST double precision, PK_USR integer) AS SELECT a.PK, b.MIN_DIST, b.MEAN_DIST, b.MAX_DIST, b.STD_DIST, a.PK_USR FROM BUILDINGS_MAPUCE a LEFT JOIN DATA_WORK.DIST b ON a.PK=b.PK"

    sql.execute "UPDATE DATA_WORK.BUILD_DIST SET MIN_DIST=0 WHERE MIN_DIST is null"
    sql.execute "UPDATE DATA_WORK.BUILD_DIST SET MEAN_DIST=0 WHERE MEAN_DIST is null"
    sql.execute "UPDATE DATA_WORK.BUILD_DIST SET MAX_DIST=0 WHERE MAX_DIST is null"
    sql.execute "UPDATE DATA_WORK.BUILD_DIST SET STD_DIST=0 WHERE STD_DIST is null"

    sql.execute "CREATE INDEX ON DATA_WORK.BUILD_DIST(PK_USR)"

    logger.warn "Compute the number of points"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_NUM_POINT (PK integer PRIMARY KEY, NUM_POINTS double precision) AS SELECT PK, (ST_NPoints(THE_GEOM) - (1+ ST_NUMINTERIORRING(THE_GEOM))) AS NUM_POINTS FROM BUILDINGS_MAPUCE"

    logger.warn "Compute the building wall near to the roads"

    def DIST_TO_ROAD = 3;

    sql.execute "CREATE TABLE DATA_WORK.ROADS_3M (THE_GEOM geometry) AS SELECT ST_BUFFER(THE_GEOM,LARGEUR + "+DIST_TO_ROAD+") as THE_GEOM FROM ROADS_MAPUCE"
    sql.execute "CREATE SPATIAL INDEX ON DATA_WORK.ROADS_3M(THE_GEOM)"


    sql.execute "CREATE TABLE DATA_WORK.BUF_GROUP (PK integer primary key, THE_GEOM geometry) AS SELECT b.PK, ST_UNION(ST_ACCUM(a.the_geom)) as THE_GEOM FROM DATA_WORK.ROADS_3M a, BUILDINGS_MAPUCE b WHERE a.THE_GEOM && b.THE_GEOM AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) GROUP BY b.PK"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_NEXT_ROAD (PK integer primary key, THE_GEOM geometry, L_TOT double precision, L_CVX double precision) AS SELECT a.PK, ST_INTERSECTION(ST_EXTERIORRING(a.THE_GEOM),b.THE_GEOM) as THE_GEOM, ROUND(a.PERIMETER,3) as L_TOT, a.L_CVX FROM BUILDINGS_MAPUCE a, DATA_WORK.BUF_GROUP b WHERE a.PK=b.PK"

    sql.execute "ALTER TABLE DATA_WORK.BUILD_NEXT_ROAD ADD COLUMN L_3M double precision as ROUND(ST_LENGTH(THE_GEOM),3)"
    sql.execute "ALTER TABLE DATA_WORK.BUILD_NEXT_ROAD ADD COLUMN L_RATIO double precision as ROUND(((L_3M*100)/L_TOT),1)"
    sql.execute "ALTER TABLE DATA_WORK.BUILD_NEXT_ROAD ADD COLUMN L_RATIO_CVX double precision as ROUND(((L_3M*100)/L_CVX),1)"




    /**
    * Compute the block of buildings and their indicators
    **/

    logger.warn "Compute block of buildings"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK AS SELECT * FROM ST_EXPLODE('SELECT PK_USR, ST_UNION(ST_ACCUM(ST_BUFFER(THE_GEOM,0.01))) as THE_GEOM FROM BUILDINGS_MAPUCE GROUP BY PK_USR')"
    sql.execute "ALTER TABLE DATA_WORK.BLOCK ADD COLUMN PK_BLOCK serial"
    sql.execute "CREATE PRIMARY KEY ON DATA_WORK.BLOCK(PK_BLOCK)"
    sql.execute "CREATE SPATIAL INDEX ON DATA_WORK.BLOCK(THE_GEOM)"
    sql.execute "CREATE INDEX ON DATA_WORK.BLOCK(PK_USR)"


    logger.warn "Compute the block matrix"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_BLOCK_MATRIX (PK_BUILD integer primary key, PK_BLOCK integer) AS SELECT a.PK AS PK_BUILD, (SELECT b.PK_BLOCK FROM DATA_WORK.BLOCK b WHERE a.THE_GEOM && b.THE_GEOM ORDER BY ST_AREA(ST_INTERSECTION(a.THE_GEOM, b.THE_GEOM)) DESC LIMIT 1) AS PK_BLOCK FROM BUILDINGS_MAPUCE a"
    sql.execute "CREATE INDEX ON DATA_WORK.BUILD_BLOCK_MATRIX(PK_BLOCK)"



    logger.warn "Finalize the building indicators table"

    sql.execute "DROP TABLE IF EXISTS BUILDING_INDICATORS"
    sql.execute "CREATE TABLE BUILDING_INDICATORS AS SELECT  a.PK, a.PK_USR , a.ID_ZONE , a.THE_GEOM, a.HAUTEUR_ORIGIN,  a.NB_NIV,a.HAUTEUR, a.AREA, a.PERIMETER, a.INSEE_INDIVIDUS, b.FLOOR_AREA, b.VOL, c.COMPACITY_R, c.COMPACITY_N, d.COMPACTNESS,e.FORM_FACTOR,f.CONCAVITY,g.MAIN_DIR_DEG,h.B_FLOOR_LONG, h.B_WALL_AREA, h.P_WALL_LONG, h.P_WALL_AREA, h.NB_NEIGHBOR, h.FREE_P_WALL_LONG, h.FREE_EXT_AREA, h.CONTIGUITY,i.P_VOL_RATIO,j.FRACTAL_DIM,k.MIN_DIST, k.MEAN_DIST, k.MAX_DIST,  k.STD_DIST, l.NUM_POINTS, m.L_TOT, a.L_CVX, m.L_3M, m.L_RATIO, m.L_RATIO_CVX,n.PK_BLOCK,a.THEME,a.PAI_BDTOPO,a.PAI_NATURE FROM BUILDINGS_MAPUCE a LEFT JOIN DATA_WORK.BUILD_AREA_VOL b ON a.PK =b.PK LEFT JOIN DATA_WORK.BUILD_COMPACITY c ON a.PK = c.PK LEFT JOIN DATA_WORK.BUILD_COMPACTNESS d ON a.PK = d.PK LEFT JOIN DATA_WORK.BUILD_FORM_FACTOR e ON a.PK = e.PK LEFT JOIN DATA_WORK.BUILD_CONCAVITY f ON a.PK = f.PK LEFT JOIN DATA_WORK.BUILD_MAIN_DIR g ON a.PK = g.PK LEFT JOIN DATA_WORK.BUILD_CONTIGUITY h ON a.PK = h.PK LEFT JOIN DATA_WORK.BUILD_P_VOL_RATIO i ON a.PK = i.PK LEFT JOIN DATA_WORK.BUILD_FRACTAL j ON a.PK = j.PK LEFT JOIN DATA_WORK.BUILD_DIST k ON a.PK = k.PK LEFT JOIN DATA_WORK.BUILD_NUM_POINT l ON a.PK =l.PK LEFT JOIN DATA_WORK.BUILD_NEXT_ROAD m ON a.PK = m.PK LEFT JOIN DATA_WORK.BUILD_BLOCK_MATRIX n ON a.PK = n.PK_BUILD"

    sql.execute "ALTER TABLE BUILDING_INDICATORS ALTER COLUMN PK SET NOT NULL"
    sql.execute "CREATE PRIMARY KEY ON BUILDING_INDICATORS(PK)"
    sql.execute "CREATE SPATIAL INDEX ON BUILDING_INDICATORS(THE_GEOM)"
    sql.execute "CREATE INDEX ON BUILDING_INDICATORS(PK_USR)"

    //Update of null parameters
    sql.execute "UPDATE BUILDING_INDICATORS SET NB_NEIGHBOR = 0 WHERE NB_NEIGHBOR is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET B_FLOOR_LONG = PERIMETER WHERE B_FLOOR_LONG is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET B_WALL_AREA = PERIMETER*HAUTEUR WHERE B_WALL_AREA is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET P_WALL_LONG = 0 WHERE P_WALL_LONG is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET P_WALL_AREA = 0 WHERE P_WALL_AREA is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET FREE_P_WALL_LONG = B_FLOOR_LONG WHERE FREE_P_WALL_LONG is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET CONTIGUITY = 0 WHERE CONTIGUITY is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET FREE_EXT_AREA = B_WALL_AREA - P_WALL_AREA + AREA WHERE FREE_EXT_AREA is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET L_TOT = ROUND (PERIMETER ,3) WHERE L_TOT is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET L_3M = 0 WHERE L_3M is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET L_RATIO = 0 WHERE L_RATIO is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET L_RATIO_CVX = 0 WHERE L_RATIO_CVX is null"
    sql.execute "UPDATE BUILDING_INDICATORS SET INSEE_INDIVIDUS = 0 WHERE INSEE_INDIVIDUS is null"


    logger.warn "Compute the sum of the building area volume by block"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_AREA_VOL (PK_BLOCK integer primary key, AREA double precision, FLOOR_AREA double precision, VOL double precision) AS SELECT a.PK_BLOCK, SUM(b.AREA) AS AREA, SUM(b.FLOOR_AREA) AS FLOOR_AREA, SUM(b.VOL) AS VOL FROM DATA_WORK.BLOCK a, BUILDING_INDICATORS b, DATA_WORK.BUILD_BLOCK_MATRIX c WHERE a.PK_BLOCK=c.PK_BLOCK AND b.PK=c.PK_BUILD GROUP BY a.PK_BLOCK"


    logger.warn "Compute the heigth statistics"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_STD_HEIGHT (PK_BLOCK integer primary key, H_MEAN double precision, H_STD double precision) AS SELECT a.PK_BLOCK, (SUM(ST_AREA(b.THE_GEOM) * b.HAUTEUR)/SUM(ST_AREA(b.THE_GEOM))) AS H_MEAN, STDDEV_POP(b.HAUTEUR) AS H_STD FROM DATA_WORK.BLOCK a, BUILDINGS_MAPUCE b, DATA_WORK.BUILD_BLOCK_MATRIX c WHERE a.PK_BLOCK=c.PK_BLOCK and b.PK=c.PK_BUILD GROUP BY a.PK_BLOCK"


    logger.warn "Compute the sum of buildings compacity"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_COMPACITY (PK_BLOCK integer primary key, COMPACITY double precision) AS SELECT a.PK_BLOCK, (SUM(b.FREE_EXT_AREA)/ POWER(SUM(b.VOL), (2./3.))) as COMPACITY FROM DATA_WORK.BLOCK a, BUILDING_INDICATORS b, DATA_WORK.BUILD_BLOCK_MATRIX c WHERE a.PK_BLOCK=c.PK_BLOCK AND b.PK=c.PK_BUILD AND b.VOL<>0 GROUP BY a.PK_BLOCK"


    logger.warn "Compute the sum of courtyards"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_COURTYARD (PK_BLOCK integer primary key, HOLES_AREA double precision, HOLES_PERCENT double precision) AS SELECT PK_BLOCK, ST_AREA(ST_HOLES(THE_GEOM)) as HOLES_AREA, (ST_AREA(ST_HOLES(THE_GEOM))/(ST_AREA(THE_GEOM)+ST_AREA(ST_HOLES(THE_GEOM)))*100) as HOLES_PERCENT FROM DATA_WORK.BLOCK"


    logger.warn "Compute the main direction of the blocks"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_MAIN_DIR (PK_BLOCK integer primary key, MAIN_DIR_DEG double precision) AS SELECT PK_BLOCK, MOD(CASEWHEN(ST_LENGTH(ST_MINIMUMDIAMETER(THE_GEOM))<0.1, DEGREES(ST_AZIMUTH(ST_STARTPOINT(THE_GEOM), ST_ENDPOINT(THE_GEOM))), DEGREES(ST_AZIMUTH(ST_STARTPOINT(ST_ROTATE(ST_MINIMUMDIAMETER(THE_GEOM),pi()/2)), ST_ENDPOINT(ST_ROTATE(ST_MINIMUMDIAMETER(THE_GEOM),pi()/2))))),180) as MAIN_DIR_DEG FROM DATA_WORK.BLOCK"


    logger.warn "Finalize the block indicators table"
    sql.execute "CREATE TABLE BLOCK_INDICATORS (PK_BLOCK integer primary key, PK_USR integer, THE_GEOM geometry, AREA double precision, FLOOR_AREA double precision, VOL double precision, H_MEAN double precision, H_STD double precision, COMPACITY double precision, HOLES_AREA double precision, HOLES_PERCENT double precision, MAIN_DIR_DEG double precision) AS SELECT a.PK_BLOCK, a.PK_USR, a.THE_GEOM, b.AREA, b.FLOOR_AREA, b.VOL,c.H_MEAN, c.H_STD,d.COMPACITY, e.HOLES_AREA, e.HOLES_PERCENT,f.MAIN_DIR_DEG FROM DATA_WORK.BLOCK a LEFT JOIN DATA_WORK.BLOCK_AREA_VOL b ON a.PK_BLOCK = b.PK_BLOCK LEFT JOIN DATA_WORK.BLOCK_STD_HEIGHT c ON a.PK_BLOCK = c.PK_BLOCK LEFT JOIN DATA_WORK.BLOCK_COMPACITY d ON a.PK_BLOCK = d.PK_BLOCK LEFT JOIN DATA_WORK.BLOCK_COURTYARD e ON a.PK_BLOCK = e.PK_BLOCK LEFT JOIN DATA_WORK.BLOCK_MAIN_DIR f ON a.PK_BLOCK = f.PK_BLOCK"
    sql.execute "CREATE INDEX ON BLOCK_INDICATORS(PK_USR)"

    logger.warn "Update the block id for each buildings"


    /**
    * Compute the USR indicators
    **/

    logger.warn "Compute the first USR indicators"
    sql.execute "CREATE TABLE DATA_WORK.USR_BUILD_TMP (PK_USR integer primary key,  COMPAC_MEAN_NW double precision, COMPAC_MEAN_W double precision,CONTIG_MEAN double precision, CONTIG_STD double precision,MAIN_DIR_STD double precision,H_MEAN double precision,H_STD double precision, P_VOL_RATIO_MEAN double precision,B_AREA double precision, B_VOL double precision, B_VOL_M double precision,BUILD_NUMB integer,MIN_M_DIST double precision, MEAN_M_DIST double precision, MEAN_STD_DIST double precision,EXT_ENV_AREA double precision)AS SELECT PK_USR,ROUND(AVG(COMPACITY_N),2) AS COMPAC_MEAN_NW, ROUND((SUM(ST_AREA(THE_GEOM) * COMPACITY_N)/SUM(ST_AREA(THE_GEOM))),2) AS COMPAC_MEAN_W,ROUND((SUM(AREA * CONTIGUITY)/SUM(AREA)),2) AS CONTIG_MEAN, ROUND(STDDEV_POP(CONTIGUITY),2) AS CONTIG_STD,ROUND(STDDEV_POP(MAIN_DIR_DEG),2) AS MAIN_DIR_STD,ROUND((SUM(AREA * HAUTEUR)/SUM(AREA)),2) AS H_MEAN,ROUND(STDDEV_POP(HAUTEUR),2) AS H_STD, ROUND((SUM(AREA * P_VOL_RATIO)/SUM(AREA)),2) AS P_VOL_RATIO_MEAN,ROUND(SUM(AREA),2) as B_AREA, ROUND(SUM(VOL),2) AS B_VOL, ROUND((SUM(VOL)/COUNT(*)),2) AS B_VOL_M,COUNT(*) as BUILD_NUMB,ROUND(AVG(MIN_DIST),2) AS MIN_M_DIST, ROUND(AVG(MEAN_DIST),2) AS MEAN_M_DIST, ROUND(STDDEV_POP(MEAN_DIST),2) AS MEAN_STD_DIST,ROUND(SUM(B_WALL_AREA-P_WALL_AREA),2) as EXT_ENV_AREA FROM BUILDING_INDICATORS GROUP BY PK_USR"

    logger.warn "Compute the floor ratio"
    sql.execute "CREATE TABLE DATA_WORK.USR_BUILD_FLOOR_RATIO (PK_USR integer primary key, FLOOR double precision, FLOOR_RATIO double precision) AS SELECT a.PK as PK_USR, ROUND(SUM(b.FLOOR_AREA),2) AS FLOOR, ROUND(SUM(b.FLOOR_AREA)/a.USR_AREA,2) AS FLOOR_RATIO FROM USR_MAPUCE a, BUILDING_INDICATORS b WHERE a.PK = b.PK_USR GROUP BY a.PK"


    logger.warn "Compute the distance for the center of the commune"
    sql.execute "CREATE TABLE DATA_WORK.USR_TO_CENTER (PK_USR integer primary key, DIST_TO_CENTER double precision) AS SELECT a.PK as PK_USR, ST_DISTANCE(ST_CENTROID(a.THE_GEOM), ST_CENTROID(b.THE_GEOM)) AS DIST_TO_CENTER FROM USR_MAPUCE a, COMMUNE_MAPUCE b"


    logger.warn "Compute the density of building areas"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA_BUILD (PK_USR integer primary key, BUILD_DENS double precision) AS SELECT a.PK as PK_USR, ROUND(SUM(b.AREA)/a.USR_AREA,4) as BUILD_DENS FROM USR_MAPUCE a, BUILDING_INDICATORS b WHERE a.PK=b.PK_USR GROUP BY a.PK"


    logger.warn "Compute the density of water surfaces"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA_HYDRO (PK_USR integer primary key, HYDRO_DENS double precision) AS SELECT PK as PK_USR, ROUND(HYDRO_SURFACE/USR_AREA,4) as HYDRO_DENS FROM USR_MAPUCE"

    logger.warn "Compute the density of vegetation"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA_VEGET (PK_USR integer primary key, VEGET_DENS double precision) AS SELECT PK as PK_USR, ROUND(VEGETATION_SURFACE/USR_AREA,4) as VEGET_DENS FROM USR_MAPUCE"

    logger.warn "Compute the density of road surfaces"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA_ROADS (PK_USR integer primary key, ROAD_DENS double precision) AS SELECT PK as PK_USR, ROUND(ROUTE_SURFACE/USR_AREA,4) as ROAD_DENS FROM USR_MAPUCE"

    logger.warn "Merging previous indicators"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA (PK_USR integer primary key, BUILD_DENS double precision, HYDRO_DENS double precision, VEGET_DENS double precision, ROAD_DENS double precision) AS SELECT a.PK as PK_USR,b.BUILD_DENS,c.HYDRO_DENS,d.VEGET_DENS,e.ROAD_DENS FROM USR_MAPUCE a LEFT JOIN DATA_WORK.USR_DENS_AREA_BUILD b ON a.PK = b.PK_USR LEFT JOIN DATA_WORK.USR_DENS_AREA_HYDRO c ON a.PK = c.PK_USR LEFT JOIN DATA_WORK.USR_DENS_AREA_VEGET d ON a.PK = d.PK_USR LEFT JOIN DATA_WORK.USR_DENS_AREA_ROADS e ON a.PK = e.PK_USR"

    logger.warn "Cleaning indicators"
    sql.execute "CREATE TABLE DATA_WORK.USR_BLOCK_TMP  (PK_USR integer primary key, B_HOLES_AREA_MEAN double precision,B_STD_H_MEAN double precision, B_M_NW_COMPACITY double precision, B_M_W_COMPACITY double precision, B_STD_COMPACITY double  precision) AS SELECT PK_USR, ROUND((SUM(AREA * HOLES_AREA)/SUM(AREA)),2) AS B_HOLES_AREA_MEAN, ROUND((SUM(AREA * H_STD)/SUM(AREA)),2) AS B_STD_H_MEAN, ROUND(SUM(COMPACITY)/COUNT(*),2) AS B_M_NW_COMPACITY, ROUND(SUM(AREA*COMPACITY)/SUM(AREA),2) AS B_M_W_COMPACITY, ROUND(STDDEV_POP(COMPACITY),2) AS B_STD_COMPACITY FROM BLOCK_INDICATORS  GROUP BY PK_USR"


    logger.warn "Finalize the USR indicators table"
    sql.execute "CREATE TABLE USR_INDICATORS  AS SELECT a.PK,a.id_zone, a.the_geom,a.insee_individus,a.insee_menages,a.insee_men_coll,a.insee_men_surf,a.insee_surface_collectif,a.vegetation_surface , a.route_surface ,a.route_longueur , a.trottoir_longueur,b.FLOOR, b.FLOOR_RATIO,c.COMPAC_MEAN_NW, c.COMPAC_MEAN_W, c.CONTIG_MEAN,c.CONTIG_STD,c.MAIN_DIR_STD,c.H_MEAN,c.H_STD,c.P_VOL_RATIO_MEAN,c.B_AREA, c.B_VOL, c.B_VOL_M,c.BUILD_NUMB, c.MIN_M_DIST, c.MEAN_M_DIST, c.MEAN_STD_DIST,m.B_HOLES_AREA_MEAN,m.B_STD_H_MEAN,m.B_M_NW_COMPACITY, m.B_M_W_COMPACITY, m.B_STD_COMPACITY, p.DIST_TO_CENTER,q.BUILD_DENS, q.HYDRO_DENS, q.VEGET_DENS, q.ROAD_DENS, c.EXT_ENV_AREA FROM USR_MAPUCE a LEFT JOIN DATA_WORK.USR_BUILD_FLOOR_RATIO b ON a.PK = b.PK_USR LEFT JOIN DATA_WORK.USR_BUILD_TMP c ON a.PK = c.PK_USR LEFT JOIN DATA_WORK.USR_BLOCK_TMP  m ON a.PK = m.PK_USR LEFT JOIN DATA_WORK.USR_TO_CENTER p ON a.PK = p.PK_USR LEFT JOIN DATA_WORK.USR_DENS_AREA q ON a.PK = q.PK_USR"

    sql.execute "ALTER TABLE USR_INDICATORS  ALTER COLUMN PK SET NOT NULL"
    sql.execute "CREATE PRIMARY KEY ON USR_INDICATORS (PK)"
    sql.execute "CREATE SPATIAL INDEX ON USR_INDICATORS (THE_GEOM)"


    sql.execute "update USR_INDICATORS set INSEE_INDIVIDUS = 0 where INSEE_INDIVIDUS is  null"
    sql.execute "update USR_INDICATORS set INSEE_MENAGES = 0 where INSEE_MENAGES is  null"
    sql.execute "update USR_INDICATORS set INSEE_MEN_COLL = 0 where INSEE_MEN_COLL is  null"
    sql.execute "update USR_INDICATORS set INSEE_MEN_SURF = 0 where INSEE_MEN_SURF is  null"
    sql.execute "update USR_INDICATORS set INSEE_SURFACE_COLLECTIF = 0 where INSEE_SURFACE_COLLECTIF is  null"
    sql.execute "update USR_INDICATORS set VEGETATION_SURFACE = 0 where VEGETATION_SURFACE is  null"
    sql.execute "update USR_INDICATORS set ROUTE_SURFACE = 0 where ROUTE_SURFACE is  null"
    sql.execute "update USR_INDICATORS set ROUTE_LONGUEUR = 0 where ROUTE_LONGUEUR is  null"
    sql.execute "update USR_INDICATORS set TROTTOIR_LONGUEUR = 0 where TROTTOIR_LONGUEUR is  null"
    sql.execute "update USR_INDICATORS set FLOOR = 0 where FLOOR is  null"
    sql.execute "update USR_INDICATORS set FLOOR_RATIO = 0 where FLOOR_RATIO is  null"
    sql.execute "update USR_INDICATORS set COMPAC_MEAN_NW = 0 where COMPAC_MEAN_NW is  null"
    sql.execute "update USR_INDICATORS set COMPAC_MEAN_W = 0 where COMPAC_MEAN_W is  null"
    sql.execute "update USR_INDICATORS set CONTIG_MEAN = 0 where CONTIG_MEAN is  null"
    sql.execute "update USR_INDICATORS set CONTIG_STD = 0 where CONTIG_STD is  null"
    sql.execute "update USR_INDICATORS set MAIN_DIR_STD = 0 where MAIN_DIR_STD is  null"
    sql.execute "update USR_INDICATORS set H_MEAN = 0 where H_MEAN is  null"
    sql.execute "update USR_INDICATORS set H_STD = 0 where H_STD is  null"
    sql.execute "update USR_INDICATORS set P_VOL_RATIO_MEAN = 0 where P_VOL_RATIO_MEAN is  null"
    sql.execute "update USR_INDICATORS set B_AREA = 0 where B_AREA is  null"
    sql.execute "update USR_INDICATORS set B_VOL = 0 where B_VOL is  null"
    sql.execute "update USR_INDICATORS set B_VOL_M = 0 where B_VOL_M is  null"
    sql.execute "update USR_INDICATORS set BUILD_NUMB = 0 where BUILD_NUMB is  null"
    sql.execute "update USR_INDICATORS set MIN_M_DIST = 0 where MIN_M_DIST is  null"
    sql.execute "update USR_INDICATORS set MEAN_M_DIST = 0 where MEAN_M_DIST is  null"
    sql.execute "update USR_INDICATORS set MEAN_STD_DIST = 0 where MEAN_STD_DIST is  null"
    sql.execute "update USR_INDICATORS set B_HOLES_AREA_MEAN = 0 where B_HOLES_AREA_MEAN is  null"
    sql.execute "update USR_INDICATORS set B_STD_H_MEAN = 0 where B_STD_H_MEAN is  null"
    sql.execute "update USR_INDICATORS set B_M_NW_COMPACITY = 0 where B_M_NW_COMPACITY is  null"
    sql.execute "update USR_INDICATORS set B_M_W_COMPACITY = 0 where B_M_W_COMPACITY is  null"
    sql.execute "update USR_INDICATORS set B_STD_COMPACITY = 0 where B_STD_COMPACITY is  null"
    sql.execute "update USR_INDICATORS set DIST_TO_CENTER = 0 where DIST_TO_CENTER is  null"
    sql.execute "update USR_INDICATORS set BUILD_DENS = 0 where BUILD_DENS is  null"
    sql.execute "update USR_INDICATORS set HYDRO_DENS = 0 where HYDRO_DENS is  null"
    sql.execute "update USR_INDICATORS set VEGET_DENS = 0 where VEGET_DENS is  null"
    sql.execute "update USR_INDICATORS set ROAD_DENS = 0 where ROAD_DENS is  null"
    sql.execute "update USR_INDICATORS set EXT_ENV_AREA = 0 where EXT_ENV_AREA is  null"   
    
     /**
    * Update the USR with the IRIS data
    **/

    sql.execute "alter table USR_INDICATORS ADD COLUMN dcomiris varchar;"

    sql.execute "update USR_INDICATORS AS a SET dcomiris =  (SELECT b.dcomiris FROM IRIS_MAPUCE b WHERE a.THE_GEOM && b.THE_GEOM ORDER BY ST_AREA(ST_INTERSECTION(st_buffer(a.THE_GEOM,0.0001), st_buffer(b.THE_GEOM, 0.0001))) DESC LIMIT 1);"
    
    sql.execute "DROP SCHEMA DATA_WORK"
   
 }
 

/**
 * Appply randomForest model to classify each buildings and USR's
 * */
def applyRandomForest(ScriptEngine engine){
        
    sql.execute "drop table if exists TMP_TYPO_BUILDINGS_MAPUCE, TMP_TYPO_USR_MAPUCE,TYPO_BUILDINGS_MAPUCE, TYPO_USR_MAPUCE";
    sql.execute "create table TMP_TYPO_BUILDINGS_MAPUCE(pk integer,  typo varchar)"
    sql.execute "create table TMP_TYPO_USR_MAPUCE(pk_usr integer,ba float,bgh float,icif float, icio float, id float, local float, pcif float, pcio float, pd float, psc float , typo_maj varchar, typo_second varchar)"
    
    r = WpsScriptsPackage.class.getResourceAsStream("scripts/randomforest_typo.R")    
    
    engine.eval(new InputStreamReader(r));    
    
    
    // Create final tables with geometries
    sql.execute "CREATE INDEX ON TMP_TYPO_BUILDINGS_MAPUCE(PK); CREATE TABLE TYPO_BUILDINGS_MAPUCE AS SELECT a.the_geom, b.pk as pk_building, a.pk_usr, a.id_zone, b.typo from BUILDINGS_MAPUCE a, TMP_TYPO_BUILDINGS_MAPUCE b where a.pk=b.pk; "
    sql.execute "CREATE INDEX ON TMP_TYPO_USR_MAPUCE(PK_USR); CREATE TABLE TYPO_USR_MAPUCE AS SELECT a.the_geom, b.pk_usr, a.id_zone,b.ba,b.bgh,b.icif, b.icio, b.id, b.local, b.pcif, b.pcio, b.pd , b.psc , b.typo_maj, b.typo_second  from USR_MAPUCE a, TMP_TYPO_USR_MAPUCE b where a.pk=b.pk_usr;"
    sql.execute "DROP TABLE IF EXISTS TMP_TYPO_BUILDINGS_MAPUCE, TMP_TYPO_USR_MAPUCE, buildings_to_predict;"
    
    sql.execute "INSERT INTO FINAL_BUILDING_TYPO (SELECT * FROM TYPO_BUILDINGS_MAPUCE);"
    sql.execute "INSERT INTO FINAL_USR_TYPO (SELECT * FROM TYPO_USR_MAPUCE);"
       

    logger.warn "The classification has been done. The tables FINAL_USR_TYPO and FINAL_BUILDING_TYPO have been created correctly" 
}


/** Login to the MApUCE database. */
@LiteralDataInput(
        title="Login to the database",
        resume="Login to the database")
String login 

/** Password to the MApUCE database. */
@PasswordInput(
        title="Password to the database",
        resume="Password to the database")
String password 


@DataFieldInput(
        title = "Spatial unit",
        resume = "Select a column to obtain a list of area identifiers : code insee or  urban area names.",
        variableReference = "\$communes_mapuce\$",
        multiSelection = false)
String[] fieldCodes

/** The list of Commune identifier */
@FieldValueInput(title="Spatial unit identifiers",
resume="Select one or more  identifiers and start the script.",
variableReference = "fieldCodes",
multiSelection = true)
String[] codesInsee


/** String output of the process. */
@LiteralDataOutput(
        title="Output message",
        resume="The geoprocessing chain has been executed.")
String literalOutput
