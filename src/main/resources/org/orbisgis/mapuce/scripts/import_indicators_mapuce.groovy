package org.orbisgis.mapuce.scripts

import org.orbisgis.orbiswpsgroovyapi.input.*
import org.orbisgis.orbiswpsgroovyapi.output.*
import org.orbisgis.orbiswpsgroovyapi.process.*
import javax.swing.JOptionPane;


/**
 * This process is used to import all indicators already computed and available in the Mapuce database
 *
 * @author Erwan Bocher
 */
@Process(title = "Import computed indicators",
    description = "Import all indicators already computed and available in the Mapuce database. Please contact info@orbigis.org to obtain an account. <br> Note : The list of available communes must be already imported. If not please execute the script to import all commune areas...",
    keywords = ["Vector","MAPuCE"])
def processing() {
    if(!login.isEmpty()&& !password.isEmpty()){
        sql.execute "SET MAX_MEMORY_ROWS 100";
        codesInsee = prepareCodes(fieldCodes,  codesInsee);  
        prepareFinalTables();
        int i=1;
        for (code in codesInsee) {
            logger.warn "Start importing the indicators for area : ${code} -> Number ${i} on ${codesInsee.length}" 
            importData(code);
            i++;
        }
        
        
        documentTables();
           
    
        literalOutput = "The data have been successfully imported."
    }

}

def documentTables(){
    sql.execute "drop table if exists  typo_label"
     //Create a table to store typo label    
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
}

/**
 * This method is used to import all data from the remote database 
 */
def importData(String code){
    
    logger.warn "Importing buildings and their indicators"    
    sql.execute "DROP TABLE IF EXISTS building_indicators_tmp"
    def schemaFromRemoteDB = "labsticc"	
    def tableFromRemoteDB = "(SELECT a.*, b.the_geom  FROM  labsticc.building_indicators_metropole as a, lienss.bati_topo as b  WHERE a.pk_building=b.pk and id_zone="+code+")"
    def query = "CREATE LINKED TABLE building_indicators_tmp ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";
    sql.execute query    
    sql.execute "INSERT INTO building_indicators_metropole (select * from building_indicators_tmp)";        
     
    sql.execute "drop table if exists building_indicators_tmp"
    
    
    logger.warn "Importing usrs and their indicators"
    sql.execute "DROP TABLE IF EXISTS usr_indicators_tmp"
    schemaFromRemoteDB = "labsticc"	
    tableFromRemoteDB = "(SELECT a.*, b.the_geom  FROM  labsticc.usr_indicators_metropole as a, lienss.usr as b  WHERE a.pk_usr=b.pk and id_zone="+code+")"
    query = "CREATE LINKED TABLE usr_indicators_tmp ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";
    sql.execute query    
    sql.execute "INSERT INTO usr_indicators_metropole (select * from usr_indicators_tmp)";        
     
    sql.execute "drop table if exists usr_indicators_tmp"
    
    
    logger.warn "Importing blocks and their indicators"
    sql.execute "DROP TABLE IF EXISTS block_indicators_tmp"
    schemaFromRemoteDB = "labsticc"	
    tableFromRemoteDB = "(select * from labsticc.block_indicators_metropole where pk_usr in (select distinct pk_usr from labsticc.usr_indicators_metropole where id_zone="+code+"))"
    query = "CREATE LINKED TABLE block_indicators_tmp ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";
    sql.execute query    
    sql.execute "INSERT INTO block_indicators_metropole (select * from block_indicators_tmp)";        
     
    sql.execute "drop table if exists block_indicators_tmp"
    
    
    logger.warn "Importing building typologies"
    sql.execute "DROP TABLE IF EXISTS typo_building_tmp"
    schemaFromRemoteDB = "labsticc"	
    tableFromRemoteDB = "(SELECT a.*, b.the_geom  FROM  labsticc.typo_building_metropole as a, lienss.bati_topo as b  WHERE a.pk_building=b.pk and a.id_zone="+code+")"
    query = "CREATE LINKED TABLE typo_building_tmp ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";
    sql.execute query    
    sql.execute "INSERT INTO typo_building_metropole (select * from typo_building_tmp)";        
     
    sql.execute "drop table if exists typo_building_tmp"
    
    
    logger.warn "Importing usr typologies"
    sql.execute "DROP TABLE IF EXISTS typo_usr_tmp"
    schemaFromRemoteDB = "labsticc"	
    tableFromRemoteDB = "(SELECT a.*, b.the_geom  FROM  labsticc.typo_usr_metropole as a, lienss.usr as b  WHERE a.pk_usr=b.pk and id_zone="+code+")"
    query = "CREATE LINKED TABLE typo_usr_tmp ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";
    sql.execute query    
    sql.execute "INSERT INTO typo_usr_metropole (select * from typo_usr_tmp)";        
     
    sql.execute "drop table if exists typo_usr_tmp"
    
    
    logger.warn "Importing commune areas"
    sql.execute "DROP TABLE IF EXISTS communes_tmp"
    schemaFromRemoteDB = "lienss"	
    tableFromRemoteDB = "(SELECT the_geom, CODE_INSEE, unite_urbaine FROM lienss.zone_etude WHERE ogc_fid="+code+")"
    query = "CREATE LINKED TABLE communes_tmp ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";
    sql.execute query    
    sql.execute "INSERT INTO communes_metropole (select * from communes_tmp)";        
     
    sql.execute "drop table if exists communes_tmp"
    
    logger.warn "Importing IRIS areas"
    sql.execute "DROP TABLE IF EXISTS iris_tmp"
    schemaFromRemoteDB = "lra"	
    tableFromRemoteDB = "(SELECT * from lra.iris_date_fm_3 WHERE id_zone="+code+")"
    query = "CREATE LINKED TABLE iris_tmp ('org.orbisgis.postgis_jts.Driver', 'jdbc:postgresql_h2://ns380291.ip-94-23-250.eu:5432/mapuce'," 
    query+=" '"+ login+"',"
    query+="'"+password+"', '"+schemaFromRemoteDB+"', "
    query+= "'"+tableFromRemoteDB+"')";
    sql.execute query    
    sql.execute "INSERT INTO iris_metropole (select * from iris_tmp)";        
     
    sql.execute "drop table if exists iris_tmp"
  
    
 }

/**
 * Prepare the tables to store all results
 * */
def prepareFinalTables(){
    sql.execute "drop table if exists iris_metropole,communes_metropole,block_indicators_metropole,building_indicators_metropole,building_bdtopo_metropole,usr_indicators_metropole,typo_usr_metropole,typo_building_metropole,typo_label;"     
    sql.execute "CREATE TABLE block_indicators_metropole (pk_block integer,   pk_usr integer,   the_geom polygon,   area double precision,   floor_area double precision,   vol double precision,   h_mean double precision,   h_std double precision,   compacity double precision,   holes_area double precision,   holes_percent double precision,   main_dir_deg double precision )"
    sql.execute "CREATE TABLE building_indicators_metropole (pk_building integer,   pk_usr integer,   id_zone integer,  hauteur_origin double precision,   nb_niv double precision,   hauteur double precision,   area double precision,   perimeter double precision,   insee_individus double precision,   floor_area double precision,   vol double precision,   compacity_r double precision,   compacity_n double precision,   compactness double precision,   form_factor double precision,   concavity double precision,   main_dir_deg double precision,   b_floor_long double precision,   b_wall_area double precision,   p_wall_long double precision,   p_wall_area double precision,   nb_neighbor double precision,   free_p_wall_long double precision,   free_ext_area double precision,   contiguity double precision,   p_vol_ratio double precision,   fractal_dim double precision,   min_dist double precision,   mean_dist double precision,   max_dist double precision,   std_dist double precision,   num_points integer,   l_tot double precision,   l_cvx double precision,   l_3m double precision,   l_ratio double precision,   l_ratio_cvx double precision,   pk_block integer,the_geom geometry )"
    sql.execute "CREATE TABLE usr_indicators_metropole (pk_usr integer NOT NULL,   id_zone integer NOT NULL,   insee_individus double precision,   insee_menages double precision,   insee_men_coll double precision,   insee_men_surf double precision,   insee_surface_collectif double precision,   vegetation_surface double precision,   route_surface double precision,   route_longueur double precision,   trottoir_longueur double precision,   floor double precision,   floor_ratio double precision,   compac_mean_nw double precision,   compac_mean_w double precision,   contig_mean double precision,   contig_std double precision,   main_dir_std double precision,   h_mean double precision,   h_std double precision,   p_vol_ratio_mean double precision,   b_area double precision,   b_vol double precision,   b_vol_m double precision,   build_numb integer,   min_m_dist double precision,   mean_m_dist double precision,   mean_std_dist double precision,   b_holes_area_mean double precision,   b_std_h_mean double precision,   b_m_nw_compacity double precision,   b_m_w_compacity double precision,   b_std_compacity double precision,   dist_to_center double precision,   build_dens double precision,   hydro_dens double precision,   veget_dens double precision,   road_dens double precision,   ext_env_area double precision,   dcomiris character varying , the_geom geometry)"
    sql.execute "CREATE TABLE typo_usr_metropole (pk_usr integer,   id_zone integer,   ba double precision,   bgh double precision,   icif double precision,   icio double precision,   id double precision,   local double precision,   pcif double precision,   pcio double precision,   pd double precision,   psc double precision,   typo_maj character varying(5),   typo_second character varying(5), the_geom geometry )"
    sql.execute "CREATE TABLE typo_building_metropole (   pk_building integer,   id_zone integer,   typo character varying(5) , the_geom geometry)"
    sql.execute "CREATE TABLE typo_label (typo character varying(5),   label character varying )"
    sql.execute "CREATE TABLE communes_metropole (the_geom geometry, CODE_INSEE character varying(10), unite_urbaine character varying(256))"
    sql.execute "CREATE TABLE iris_metropole ( pk integer,  the_geom geometry,  depcom character varying(5),    nom_com character varying(45),   dcomiris character varying(9),    m_49_74 double precision,  m_75_89 double precision,    m_90_04 double precision, a_49_74 double precision,    a_75_89 double precision, a_90_04 double precision,    m_a1946 double precision,  a_a1946 double precision,  m_46_49 double precision,  a_46_49 integer,   m_05_08 integer, a_05_08 integer,    m_09_11 integer,  a_09_11 integer,  id_zone integer)"
}


/**
 * Returns an array with all insee codes even if the selected field is for urban areas.
 *
 **/
def prepareCodes(String[] fieldCodes, String[] codesInsee ){
    if (fieldCodes[0].equalsIgnoreCase("unite_urbaine")){
        def codesUU = []
        sql.eachRow("select id_zone from COMMUNES_MAPUCE where unite_urbaine in(${codesInsee.join(',')});"){row ->
            codesUU.add(row.id_zone)            
        }
        codesInsee = codesUU as String[]
    }
    else{
        def codesUU = []
        sql.eachRow("select id_zone from COMMUNES_MAPUCE where code_insee in(${codesInsee.join(',')});"){row ->
            codesUU.add(row.id_zone)            
        }
        codesInsee = codesUU as String[]
    }
    return codesInsee;
}


/** Login to the MApUCE database. */
@LiteralDataInput(
    title="Login to the database",
    description="Login to the database")
String login 

/** Password to the MApUCE database. */
@PasswordInput(
    title="Password to the database",
    description="Password to the database")
String password 

@JDBCColumnInput(
    title = "Spatial unit",
    description = "Select a column to obtain a list of area identifiers : code insee or  urban area names.",
    jdbcTableReference = "\$communes_mapuce\$",
    multiSelection = false)
String[] fieldCodes

/** The list of Commune identifier */
@JDBCValueInput(title="Spatial unit identifiers",
    description="Select one or more  identifiers and start the script.",
    jdbcColumnReference = "fieldCodes",
    multiSelection = true)
String[] codesInsee


/** String output of the process. */
@LiteralDataOutput(
    title="Output message",
    description="The output message")
String literalOutput
