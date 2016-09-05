import org.orbisgis.wpsgroovyapi.input.*
import org.orbisgis.wpsgroovyapi.output.*
import org.orbisgis.wpsgroovyapi.process.*
import java.sql.SQLException;
import org.h2gis.utilities.*;


/**
 * This process compute the morphological indicators needed for the
 * MApUCE project. These indicators are compute for a french NUTS.
 * 
 * @return 3 files that contains the indicators at USR, BLOCK and 
 * BUILDING scales.
 *
 * @author Erwan Bocher
 */
@Process(title = "3-Compute morphological indicators",
        resume = "Compute a set of morphological indicators based on the french national vector database called BD Topo.<br> The indicators are computed at 3 levels of spatial unit : building, block, USR and stored in 3 tables.<br> <ul> <li>A building is the main geometry (output table name  : BUILDING_INDICATORS).</li> <li> A block represents the union of a set of touching geometry buildings (output table name  : BLOCK_INDICATORS).</li> <li> USR (Unité Spatiale de Référence – Reference Spatial Unit) is the smallest area that is surrounded by streets (output table name  : USR_INDICATORS).</li></ul><br>Note : Please use the extract metadata script in the toolbox to get some basic information on the indicators stored in the 3 output tables.",
        keywords = ["Vector","MAPuCE"])
def processing() {

    sql.execute "DROP TABLE IF EXISTS BUILDING_INDICATORS, USR_INDICATORS, BLOCK_INDICATORS"
    sql.execute "DROP SCHEMA IF EXISTS DATA_WORK"
    sql.execute "CREATE SCHEMA DATA_WORK"

    /**
    * Compute the building indicators
    **/

    logger.warn "Compute area volume"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_AREA_VOL (PK integer primary key, FLOOR_AREA double, VOL double) AS SELECT PK, (AREA * NB_NIV) AS FLOOR_AREA , (AREA * HAUTEUR) AS VOL FROM BUILDINGS_MAPUCE"

    logger.warn "Compute form factor"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_FORM_FACTOR (PK integer primary key , FORM_FACTOR double) AS SELECT PK , AREA / POWER(PERIMETER ,2) AS FORM_FACTOR FROM BUILDINGS_MAPUCE"

    logger.warn "Compute concavity"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_CONCAVITY (PK integer primary key, CONCAVITY double) AS SELECT PK, (AREA / ST_AREA(ST_CONVEXHULL(THE_GEOM))) AS CONCAVITY FROM BUILDINGS_MAPUCE"

    logger.warn "Compute contiguity"


    sql.execute "CREATE TABLE DATA_WORK.CONTIGUITY_INTERSECTION AS SELECT a.PK as PK_D, a.HAUTEUR as H_D, b.PK as PK_G, b.HAUTEUR as H_G, ST_AREA(a.THE_GEOM) as B_FLOOR_AREA, a.PERIMETER as B_FLOOR_LONG, (a.PERIMETER * a.HAUTEUR) as B_WALL_AREA, ST_INTERSECTION(a.THE_GEOM, b.THE_GEOM) as THE_GEOM FROM BUILDINGS_MAPUCE a, BUILDINGS_MAPUCE b WHERE a.PK <> b.PK AND a.THE_GEOM && b.THE_GEOM AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM)"


    sql.execute "CREATE TABLE DATA_WORK.BUILD_CONTIGUITY_GEOM AS SELECT PK_D as PK, B_FLOOR_AREA, B_FLOOR_LONG, B_WALL_AREA, LEAST(H_D, H_G) as H_MIN, ST_LENGTH(ST_COLLECTIONEXTRACT(THE_GEOM,2)) as P_WALL_LONG, (LEAST(H_D, H_G) * ST_LENGTH(ST_COLLECTIONEXTRACT(THE_GEOM,2))) as P_WALL_AREA FROM DATA_WORK.CONTIGUITY_INTERSECTION"
    sql.execute "CREATE INDEX ON DATA_WORK.BUILD_CONTIGUITY_GEOM(PK)"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_CONTIGUITY (PK integer primary key, B_FLOOR_AREA double, B_FLOOR_LONG double, B_WALL_AREA double, P_WALL_LONG double, P_WALL_AREA double, NB_NEIGHBOR integer) AS SELECT PK, B_FLOOR_AREA, B_FLOOR_LONG, B_WALL_AREA, SUM(P_WALL_LONG) as P_WALL_LONG, SUM(P_WALL_AREA) as P_WALL_AREA, COUNT(*) as NB_NEIGHBOR FROM DATA_WORK.BUILD_CONTIGUITY_GEOM GROUP BY PK, B_FLOOR_AREA, B_FLOOR_LONG, B_WALL_AREA"

    sql.execute "ALTER TABLE DATA_WORK.BUILD_CONTIGUITY ADD COLUMN FREE_P_WALL_LONG double as B_FLOOR_LONG-P_WALL_LONG"
    sql.execute "ALTER TABLE DATA_WORK.BUILD_CONTIGUITY ADD COLUMN FREE_EXT_AREA double as (B_WALL_AREA - P_WALL_AREA + B_FLOOR_AREA)"
    sql.execute "ALTER TABLE DATA_WORK.BUILD_CONTIGUITY ADD COLUMN CONTIGUITY double as CASEWHEN(B_WALL_AREA>0, P_WALL_AREA/B_WALL_AREA, null)"

    logger.warn "Compute compacity";

    sql.execute "CREATE TABLE DATA_WORK.BUILD_COMPACITY_TMP AS SELECT a.PK, a.THE_GEOM, a.HAUTEUR, b.FREE_EXT_AREA FROM BUILDINGS_MAPUCE a LEFT JOIN DATA_WORK.BUILD_CONTIGUITY b ON a.PK=b.PK"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_COMPACITY (PK integer primary key, COMPACITY_R double, COMPACITY_N double) AS SELECT PK, (((ST_PERIMETER(THE_GEOM) * HAUTEUR) + ST_AREA(THE_GEOM)) / (POWER(ST_AREA(THE_GEOM) * HAUTEUR, (2./3.)))) as COMPACITY_R, (FREE_EXT_AREA/(POWER(ST_AREA(THE_GEOM) * HAUTEUR, (2./3.)))) as COMPACITY_N FROM DATA_WORK.BUILD_COMPACITY_TMP WHERE ST_AREA(THE_GEOM)!=0 AND HAUTEUR !=0"

    // If a building has no neighbors, so the net compacity is equal to the raw compacity
    sql.execute "UPDATE DATA_WORK.BUILD_COMPACITY SET COMPACITY_N = COMPACITY_R WHERE COMPACITY_N is null"

    logger.warn "Compute compactness"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_COMPACTNESS (PK integer primary key, COMPACTNESS double) AS SELECT PK, (PERIMETER/(2 * SQRT(PI() * AREA))) AS COMPACTNESS FROM BUILDINGS_MAPUCE"

    logger.warn "Compute main direction"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_MAIN_DIR (PK integer primary key, MAIN_DIR_DEG double) AS SELECT PK, MOD(CASEWHEN(ST_LENGTH(ST_MINIMUMDIAMETER(THE_GEOM))<0.1, DEGREES(ST_AZIMUTH(ST_STARTPOINT(THE_GEOM), ST_ENDPOINT(THE_GEOM))), DEGREES(ST_AZIMUTH(ST_STARTPOINT(ST_ROTATE(ST_MINIMUMDIAMETER(THE_GEOM),pi()/2)), ST_ENDPOINT(ST_ROTATE(ST_MINIMUMDIAMETER(THE_GEOM),pi()/2))))),180) as ROADS_MIN_DIR FROM BUILDINGS_MAPUCE"

    logger.warn "Compute passive volume ratio"

    // Initialisation of parameters
    // Tolerance distance (in meters)
    def EPSILON = 0.005
    // Size of the buffer (in meters)
    def BUFFER_IN = 6 + EPSILON

    sql.execute "CREATE TABLE DATA_WORK.merged_buildings(PK integer primary key, THE_GEOM geometry) AS SELECT ba.PK, ST_UNION(ST_BUFFER(ba.the_geom,"+EPSILON+", 'join=mitre'), ST_UNION(ST_ACCUM(ST_BUFFER(bb.the_geom,"+EPSILON+", 'join=mitre')))) geom FROM BUILDINGS_MAPUCE ba, BUILDINGS_MAPUCE bb WHERE ba.the_geom && ST_EXPAND(bb.the_geom,1,1) AND ba.PK <> bb.PK AND ST_INTERSECTS(ST_BUFFER(ba.the_geom,"+EPSILON+", 'join=mitre'), bb.the_geom) GROUP BY ba.PK, ba.the_geom"

    sql.execute "CREATE TABLE DATA_WORK.in_buffer(PK integer primary key, THE_GEOM geometry) AS SELECT ba.pk, ST_INTERSECTION(ST_BUFFER(ba.the_geom,2 * "+EPSILON+", 'join=mitre') ,ST_BUFFER(ST_SIDEBUFFER(CASEWHEN(mb.PK is null,ST_TOMULTISEGMENTS(ba.the_geom),ST_INTERSECTION(ST_BUFFER(ba.the_geom,10 * "+EPSILON+", 'join=mitre') ,ST_UNION(ST_TOMULTISEGMENTS(ST_HOLES(mb.the_geom)), ST_TOMULTISEGMENTS(ST_EXTERIORRING(mb.the_geom))))), -"+BUFFER_IN+"),"+EPSILON+", 'join=mitre')) the_geom FROM BUILDINGS_MAPUCE BA LEFT JOIN DATA_WORK.merged_buildings mb ON ba.PK = mb.PK"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_P_VOL_RATIO (PK integer PRIMARY KEY, THE_GEOM geometry, P_VOL_RATIO double) AS SELECT a.PK, b.THE_GEOM, ROUND(LEAST((ST_AREA(b.THE_GEOM)/AREA),1),2) as P_VOL_RATIO FROM BUILDINGS_MAPUCE a, DATA_WORK.in_buffer b WHERE a.PK=b.PK"

    logger.warn "Compute fractal dimension"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_FRACTAL (PK integer PRIMARY KEY, FRACTAL_DIM double) AS SELECT PK, (2 * LOG(PERIMETER)) / LOG(AREA) AS FRACTAL_DIM FROM BUILDINGS_MAPUCE WHERE AREA<>1"

    logger.warn "Compute the distance between buildings"

    sql.execute "CREATE TABLE DATA_WORK.DIST (PK integer PRIMARY KEY, MIN_DIST double,  MEAN_DIST double, MAX_DIST double, STD_DIST double, PK_USR integer)AS SELECT a.PK, MIN(ST_DISTANCE(a.THE_GEOM, b.THE_GEOM)) AS MIN_DIST, AVG(ST_DISTANCE(a.THE_GEOM, b.THE_GEOM)) AS MEAN_DIST, MAX(ST_DISTANCE(a.THE_GEOM, b.THE_GEOM)) AS MAX_DIST, STDDEV_POP(ST_DISTANCE(a.THE_GEOM, b.THE_GEOM)) AS STD_DIST,a.PK_USR FROM BUILDINGS_MAPUCE a, BUILDINGS_MAPUCE b WHERE a.PK<>b.PK AND a.PK_USR=b.PK_USR GROUP BY a.PK_USR, a.PK"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_DIST (PK integer PRIMARY KEY, MIN_DIST double,  MEAN_DIST double, MAX_DIST double, STD_DIST double, PK_USR integer) AS SELECT a.PK, b.MIN_DIST, b.MEAN_DIST, b.MAX_DIST, b.STD_DIST, a.PK_USR FROM BUILDINGS_MAPUCE a LEFT JOIN DATA_WORK.DIST b ON a.PK=b.PK"

    sql.execute "UPDATE DATA_WORK.BUILD_DIST SET MIN_DIST=0 WHERE MIN_DIST is null"
    sql.execute "UPDATE DATA_WORK.BUILD_DIST SET MEAN_DIST=0 WHERE MEAN_DIST is null"
    sql.execute "UPDATE DATA_WORK.BUILD_DIST SET MAX_DIST=0 WHERE MAX_DIST is null"
    sql.execute "UPDATE DATA_WORK.BUILD_DIST SET STD_DIST=0 WHERE STD_DIST is null"

    sql.execute "CREATE INDEX ON DATA_WORK.BUILD_DIST(PK_USR)"

    logger.warn "Compute the number of points"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_NUM_POINT (PK integer PRIMARY KEY, NUM_POINTS double) AS SELECT PK, (ST_NPoints(THE_GEOM) - (1+ ST_NUMINTERIORRING(THE_GEOM))) AS NUM_POINTS FROM BUILDINGS_MAPUCE"

    logger.warn "Compute the building wall near to the roads"

    def DIST_TO_ROAD = 3;

    sql.execute "CREATE TABLE DATA_WORK.ROADS_3M (THE_GEOM geometry) AS SELECT ST_BUFFER(THE_GEOM,LARGEUR + "+DIST_TO_ROAD+") as THE_GEOM FROM ROADS_MAPUCE"
    sql.execute "CREATE SPATIAL INDEX ON DATA_WORK.ROADS_3M(THE_GEOM)"


    sql.execute "CREATE TABLE DATA_WORK.BUF_GROUP (PK integer primary key, THE_GEOM geometry) AS SELECT b.PK, ST_UNION(ST_ACCUM(a.the_geom)) as THE_GEOM FROM DATA_WORK.ROADS_3M a, BUILDINGS_MAPUCE b WHERE a.THE_GEOM && b.THE_GEOM AND ST_INTERSECTS(a.THE_GEOM, b.THE_GEOM) GROUP BY b.PK"

    sql.execute "CREATE TABLE DATA_WORK.BUILD_NEXT_ROAD (PK integer primary key, THE_GEOM geometry, L_TOT double, L_CVX double) AS SELECT a.PK, ST_INTERSECTION(ST_EXTERIORRING(a.THE_GEOM),b.THE_GEOM) as THE_GEOM, ROUND(a.PERIMETER,3) as L_TOT, a.L_CVX FROM BUILDINGS_MAPUCE a, DATA_WORK.BUF_GROUP b WHERE a.PK=b.PK"

    sql.execute "ALTER TABLE DATA_WORK.BUILD_NEXT_ROAD ADD COLUMN L_3M double as ROUND(ST_LENGTH(THE_GEOM),3)"
    sql.execute "ALTER TABLE DATA_WORK.BUILD_NEXT_ROAD ADD COLUMN L_RATIO double as ROUND(((L_3M*100)/L_TOT),1)"
    sql.execute "ALTER TABLE DATA_WORK.BUILD_NEXT_ROAD ADD COLUMN L_RATIO_CVX double as ROUND(((L_3M*100)/L_CVX),1)"




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
    sql.execute "CREATE TABLE BUILDING_INDICATORS AS SELECT a.THE_GEOM, a.HAUTEUR_ORIGIN, a.ID_ZONE , a.PK_USR , a.PK, a.NB_NIV,a.HAUTEUR, a.AREA, a.PERIMETER, a.INSEE_INDIVIDUS, b.FLOOR_AREA, b.VOL, c.COMPACITY_R, c.COMPACITY_N, d.COMPACTNESS,e.FORM_FACTOR,f.CONCAVITY,g.MAIN_DIR_DEG,h.B_FLOOR_LONG, h.B_WALL_AREA, h.P_WALL_LONG, h.P_WALL_AREA, h.NB_NEIGHBOR, h.FREE_P_WALL_LONG, h.FREE_EXT_AREA, h.CONTIGUITY,i.P_VOL_RATIO,j.FRACTAL_DIM,k.MIN_DIST, k.MEAN_DIST, k.MAX_DIST,  k.STD_DIST, l.NUM_POINTS, m.L_TOT, a.L_CVX, m.L_3M, m.L_RATIO, m.L_RATIO_CVX,n.PK_BLOCK FROM BUILDINGS_MAPUCE a LEFT JOIN DATA_WORK.BUILD_AREA_VOL b ON a.PK =b.PK LEFT JOIN DATA_WORK.BUILD_COMPACITY c ON a.PK = c.PK LEFT JOIN DATA_WORK.BUILD_COMPACTNESS d ON a.PK = d.PK LEFT JOIN DATA_WORK.BUILD_FORM_FACTOR e ON a.PK = e.PK LEFT JOIN DATA_WORK.BUILD_CONCAVITY f ON a.PK = f.PK LEFT JOIN DATA_WORK.BUILD_MAIN_DIR g ON a.PK = g.PK LEFT JOIN DATA_WORK.BUILD_CONTIGUITY h ON a.PK = h.PK LEFT JOIN DATA_WORK.BUILD_P_VOL_RATIO i ON a.PK = i.PK LEFT JOIN DATA_WORK.BUILD_FRACTAL j ON a.PK = j.PK LEFT JOIN DATA_WORK.BUILD_DIST k ON a.PK = k.PK LEFT JOIN DATA_WORK.BUILD_NUM_POINT l ON a.PK =l.PK LEFT JOIN DATA_WORK.BUILD_NEXT_ROAD m ON a.PK = m.PK LEFT JOIN DATA_WORK.BUILD_BLOCK_MATRIX n ON a.PK = n.PK_BUILD"

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
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_AREA_VOL (PK_BLOCK integer primary key, AREA double, FLOOR_AREA double, VOL double) AS SELECT a.PK_BLOCK, SUM(b.AREA) AS AREA, SUM(b.FLOOR_AREA) AS FLOOR_AREA, SUM(b.VOL) AS VOL FROM DATA_WORK.BLOCK a, BUILDING_INDICATORS b, DATA_WORK.BUILD_BLOCK_MATRIX c WHERE a.PK_BLOCK=c.PK_BLOCK AND b.PK=c.PK_BUILD GROUP BY a.PK_BLOCK"


    logger.warn "Compute the heigth statistics"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_STD_HEIGHT (PK_BLOCK integer primary key, H_MEAN double, H_STD double) AS SELECT a.PK_BLOCK, (SUM(ST_AREA(b.THE_GEOM) * b.HAUTEUR)/SUM(ST_AREA(b.THE_GEOM))) AS H_MEAN, STDDEV_POP(b.HAUTEUR) AS H_STD FROM DATA_WORK.BLOCK a, BUILDINGS_MAPUCE b, DATA_WORK.BUILD_BLOCK_MATRIX c WHERE a.PK_BLOCK=c.PK_BLOCK and b.PK=c.PK_BUILD GROUP BY a.PK_BLOCK"


    logger.warn "Compute the sum of buildings compacity"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_COMPACITY (PK_BLOCK integer primary key, COMPACITY double) AS SELECT a.PK_BLOCK, (SUM(b.FREE_EXT_AREA)/ POWER(SUM(b.VOL), (2./3.))) as COMPACITY FROM DATA_WORK.BLOCK a, BUILDING_INDICATORS b, DATA_WORK.BUILD_BLOCK_MATRIX c WHERE a.PK_BLOCK=c.PK_BLOCK AND b.PK=c.PK_BUILD AND b.VOL<>0 GROUP BY a.PK_BLOCK"


    logger.warn "Compute the sum of courtyards"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_COURTYARD (PK_BLOCK integer primary key, HOLES_AREA double, HOLES_PERCENT double) AS SELECT PK_BLOCK, ST_AREA(ST_HOLES(THE_GEOM)) as HOLES_AREA, (ST_AREA(ST_HOLES(THE_GEOM))/(ST_AREA(THE_GEOM)+ST_AREA(ST_HOLES(THE_GEOM)))*100) as HOLES_PERCENT FROM DATA_WORK.BLOCK"


    logger.warn "Compute the main direction of the blocks"
    sql.execute "CREATE TABLE DATA_WORK.BLOCK_MAIN_DIR (PK_BLOCK integer primary key, MAIN_DIR_DEG double) AS SELECT PK_BLOCK, MOD(CASEWHEN(ST_LENGTH(ST_MINIMUMDIAMETER(THE_GEOM))<0.1, DEGREES(ST_AZIMUTH(ST_STARTPOINT(THE_GEOM), ST_ENDPOINT(THE_GEOM))), DEGREES(ST_AZIMUTH(ST_STARTPOINT(ST_ROTATE(ST_MINIMUMDIAMETER(THE_GEOM),pi()/2)), ST_ENDPOINT(ST_ROTATE(ST_MINIMUMDIAMETER(THE_GEOM),pi()/2))))),180) as MAIN_DIR_DEG FROM DATA_WORK.BLOCK"


    logger.warn "Finalize the block indicators table"
    sql.execute "CREATE TABLE BLOCK_INDICATORS (THE_GEOM geometry, PK_BLOCK integer primary key, PK_USR integer, AREA double, FLOOR_AREA double, VOL double, H_MEAN double, H_STD double, COMPACITY double, HOLES_AREA double, HOLES_PERCENT double, MAIN_DIR_DEG double) AS SELECT a.THE_GEOM, a.PK_BLOCK, a.PK_USR, b.AREA, b.FLOOR_AREA, b.VOL,c.H_MEAN, c.H_STD,d.COMPACITY, e.HOLES_AREA, e.HOLES_PERCENT,f.MAIN_DIR_DEG FROM DATA_WORK.BLOCK a LEFT JOIN DATA_WORK.BLOCK_AREA_VOL b ON a.PK_BLOCK = b.PK_BLOCK LEFT JOIN DATA_WORK.BLOCK_STD_HEIGHT c ON a.PK_BLOCK = c.PK_BLOCK LEFT JOIN DATA_WORK.BLOCK_COMPACITY d ON a.PK_BLOCK = d.PK_BLOCK LEFT JOIN DATA_WORK.BLOCK_COURTYARD e ON a.PK_BLOCK = e.PK_BLOCK LEFT JOIN DATA_WORK.BLOCK_MAIN_DIR f ON a.PK_BLOCK = f.PK_BLOCK"
    sql.execute "CREATE INDEX ON BLOCK_INDICATORS(PK_USR)"

    logger.warn "Update the block id for each buildings"


    /**
    * Compute the USR indicators
    **/

    logger.warn "Compute the first USR indicators"
    sql.execute "CREATE TABLE DATA_WORK.USR_BUILD_TMP (PK_USR integer primary key,  COMPAC_MEAN_NW double, COMPAC_MEAN_W double,CONTIG_MEAN double, CONTIG_STD double,MAIN_DIR_STD double,H_MEAN double,H_STD double, P_VOL_RATIO_MEAN double,B_AREA double, B_VOL double, B_VOL_M double,BUILD_NUMB integer,MIN_M_DIST double, MEAN_M_DIST double, MEAN_STD_DIST double,EXT_ENV_AREA double)AS SELECT PK_USR,ROUND(AVG(COMPACITY_N),2) AS COMPAC_MEAN_NW, ROUND((SUM(ST_AREA(THE_GEOM) * COMPACITY_N)/SUM(ST_AREA(THE_GEOM))),2) AS COMPAC_MEAN_W,ROUND((SUM(AREA * CONTIGUITY)/SUM(AREA)),2) AS CONTIG_MEAN, ROUND(STDDEV_POP(CONTIGUITY),2) AS CONTIG_STD,ROUND(STDDEV_POP(MAIN_DIR_DEG),2) AS MAIN_DIR_STD,ROUND((SUM(AREA * HAUTEUR)/SUM(AREA)),2) AS H_MEAN,ROUND(STDDEV_POP(HAUTEUR),2) AS H_STD, ROUND((SUM(AREA * P_VOL_RATIO)/SUM(AREA)),2) AS P_VOL_RATIO_MEAN,ROUND(SUM(AREA),2) as B_AREA, ROUND(SUM(VOL),2) AS B_VOL, ROUND((SUM(VOL)/COUNT(*)),2) AS B_VOL_M,COUNT(*) as BUILD_NUMB,ROUND(AVG(MIN_DIST),2) AS MIN_M_DIST, ROUND(AVG(MEAN_DIST),2) AS MEAN_M_DIST, ROUND(STDDEV_POP(MEAN_DIST),2) AS MEAN_STD_DIST,ROUND(SUM(B_WALL_AREA-P_WALL_AREA),2) as EXT_ENV_AREA FROM BUILDING_INDICATORS GROUP BY PK_USR"

    logger.warn "Compute the floor ratio"
    sql.execute "CREATE TABLE DATA_WORK.USR_BUILD_FLOOR_RATIO (PK_USR integer primary key, FLOOR double, FLOOR_RATIO double) AS SELECT a.PK as PK_USR, ROUND(SUM(b.FLOOR_AREA),2) AS FLOOR, ROUND(SUM(b.FLOOR_AREA)/a.USR_AREA,2) AS FLOOR_RATIO FROM USR_MAPUCE a, BUILDING_INDICATORS b WHERE a.PK = b.PK_USR GROUP BY a.PK"


    logger.warn "Compute the distance for the center of the commune"
    sql.execute "CREATE TABLE DATA_WORK.USR_TO_CENTER (PK_USR integer primary key, DIST_TO_CENTER double) AS SELECT a.PK as PK_USR, ST_DISTANCE(ST_CENTROID(a.THE_GEOM), ST_CENTROID(b.THE_GEOM)) AS DIST_TO_CENTER FROM USR_MAPUCE a, COMMUNE_MAPUCE b"


    logger.warn "Compute the density of building areas"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA_BUILD (PK_USR integer primary key, BUILD_DENS double) AS SELECT a.PK as PK_USR, ROUND(SUM(b.AREA)/a.USR_AREA,4) as BUILD_DENS FROM USR_MAPUCE a, BUILDING_INDICATORS b WHERE a.PK=b.PK_USR GROUP BY a.PK"


    logger.warn "Compute the density of water surfaces"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA_HYDRO (PK_USR integer primary key, HYDRO_DENS double) AS SELECT PK as PK_USR, ROUND(HYDRO_SURFACE/USR_AREA,4) as HYDRO_DENS FROM USR_MAPUCE"

    logger.warn "Compute the density of vegetation"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA_VEGET (PK_USR integer primary key, VEGET_DENS double) AS SELECT PK as PK_USR, ROUND(VEGETATION_SURFACE/USR_AREA,4) as VEGET_DENS FROM USR_MAPUCE"

    logger.warn "Compute the density of road surfaces"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA_ROADS (PK_USR integer primary key, ROAD_DENS double) AS SELECT PK as PK_USR, ROUND(ROUTE_SURFACE/USR_AREA,4) as ROAD_DENS FROM USR_MAPUCE"

    logger.warn "Merging previous indicators"
    sql.execute "CREATE TABLE DATA_WORK.USR_DENS_AREA (PK_USR integer primary key, BUILD_DENS double, HYDRO_DENS double, VEGET_DENS double, ROAD_DENS double) AS SELECT a.PK as PK_USR,b.BUILD_DENS,c.HYDRO_DENS,d.VEGET_DENS,e.ROAD_DENS FROM USR_MAPUCE a LEFT JOIN DATA_WORK.USR_DENS_AREA_BUILD b ON a.PK = b.PK_USR LEFT JOIN DATA_WORK.USR_DENS_AREA_HYDRO c ON a.PK = c.PK_USR LEFT JOIN DATA_WORK.USR_DENS_AREA_VEGET d ON a.PK = d.PK_USR LEFT JOIN DATA_WORK.USR_DENS_AREA_ROADS e ON a.PK = e.PK_USR"

    logger.warn "Cleaning indicators"
    sql.execute "CREATE TABLE DATA_WORK.USR_BLOCK_TMP  (PK_USR integer primary key, B_HOLES_AREA_MEAN double,B_STD_H_MEAN double, B_M_NW_COMPACITY double, B_M_W_COMPACITY double, B_STD_COMPACITY double) AS SELECT PK_USR, ROUND((SUM(AREA * HOLES_AREA)/SUM(AREA)),2) AS B_HOLES_AREA_MEAN, ROUND((SUM(AREA * H_STD)/SUM(AREA)),2) AS B_STD_H_MEAN, ROUND(SUM(COMPACITY)/COUNT(*),2) AS B_M_NW_COMPACITY, ROUND(SUM(AREA*COMPACITY)/SUM(AREA),2) AS B_M_W_COMPACITY, ROUND(STDDEV_POP(COMPACITY),2) AS B_STD_COMPACITY FROM BLOCK_INDICATORS  GROUP BY PK_USR"


    logger.warn "Finalize the USR indicators table"
    sql.execute "CREATE TABLE USR_INDICATORS  AS SELECT a.PK,a.the_geom,a.id_zone,a.insee_individus,a.insee_menages,a.insee_men_coll,a.insee_men_surf,a.insee_surface_collectif,a.vegetation_surface , a.route_surface ,a.route_longueur , a.trottoir_longueur,b.FLOOR, b.FLOOR_RATIO,c.COMPAC_MEAN_NW, c.COMPAC_MEAN_W, c.CONTIG_MEAN,c.CONTIG_STD,c.MAIN_DIR_STD,c.H_MEAN,c.H_STD,c.P_VOL_RATIO_MEAN,c.B_AREA, c.B_VOL, c.B_VOL_M,c.BUILD_NUMB, c.MIN_M_DIST, c.MEAN_M_DIST, c.MEAN_STD_DIST,m.B_HOLES_AREA_MEAN,m.B_STD_H_MEAN,m.B_M_NW_COMPACITY, m.B_M_W_COMPACITY, m.B_STD_COMPACITY, p.DIST_TO_CENTER,q.BUILD_DENS, q.HYDRO_DENS, q.VEGET_DENS, q.ROAD_DENS, c.EXT_ENV_AREA FROM USR_MAPUCE a LEFT JOIN DATA_WORK.USR_BUILD_FLOOR_RATIO b ON a.PK = b.PK_USR LEFT JOIN DATA_WORK.USR_BUILD_TMP c ON a.PK = c.PK_USR LEFT JOIN DATA_WORK.USR_BLOCK_TMP  m ON a.PK = m.PK_USR LEFT JOIN DATA_WORK.USR_TO_CENTER p ON a.PK = p.PK_USR LEFT JOIN DATA_WORK.USR_DENS_AREA q ON a.PK = q.PK_USR"

    sql.execute "ALTER TABLE USR_INDICATORS  ALTER COLUMN PK SET NOT NULL"
    sql.execute "CREATE PRIMARY KEY ON USR_INDICATORS (PK)"
    sql.execute "CREATE SPATIAL INDEX ON USR_INDICATORS (THE_GEOM)"


    sql.execute "UPDATE USR_INDICATORS  SET FLOOR = 0 WHERE FLOOR is null"
    sql.execute "UPDATE USR_INDICATORS  SET FLOOR_RATIO = 0 WHERE FLOOR_RATIO is null"
    sql.execute "UPDATE USR_INDICATORS  SET B_AREA = 0 WHERE B_AREA is null"
    sql.execute "UPDATE USR_INDICATORS  SET B_VOL = 0 WHERE B_VOL is null"
    sql.execute "UPDATE USR_INDICATORS  SET BUILD_DENS = 0 WHERE BUILD_DENS is null"
    sql.execute "UPDATE USR_INDICATORS  SET EXT_ENV_AREA = 0 WHERE EXT_ENV_AREA is null"
    sql.execute "UPDATE USR_INDICATORS  SET insee_individus=0 WHERE insee_individus is null"
    sql.execute "UPDATE USR_INDICATORS  SET insee_menages=0 where insee_menages is null"
    sql.execute "UPDATE USR_INDICATORS  SET insee_men_coll=0 where insee_men_coll is null"
    sql.execute "UPDATE USR_INDICATORS  SET insee_men_surf=0 where insee_men_surf is null"
    sql.execute "UPDATE USR_INDICATORS  SET insee_surface_collectif=0 where insee_surface_collectif is null"
    logger.warn "Cleaning the database"
    sql.execute "DROP SCHEMA DATA_WORK"
    
    /**
    * Indicators definition
    **/
    sql.execute "COMMENT ON COLUMN BLOCK_INDICATORS.the_geom IS ' External border of the union of a set of touching geometry buildings';"
    sql.execute "COMMENT ON COLUMN BLOCK_INDICATORS.pk_block IS 'Unique identifier for a block geometry';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.pk_usr IS 'Unique identifier of the usr';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.area IS 'Area of the block';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.floor_area IS 'Sum of building the floor areas in the block';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.vol IS 'Sum of the building volumes in a block';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.h_mean IS 'Buildings’s mean height in each block';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.h_std IS 'Buildings’s Standard Deviation height in each block';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.compacity IS 'The block’s compacity is defined as the sum of the building’s external surfaces divided by the sum of building’s volume power two-third';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.holes_area IS ' Sum of holes’s area (courtyard) in a block';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.holes_percent IS 'Compute the ratio (percent) of courtyard area in a block of buildings';"
    sql.execute  "COMMENT ON COLUMN BLOCK_INDICATORS.main_dir_deg IS 'The main direction corresponds to the direction (in degree) given by the longest side of the geometry’s minimum rectangle. The north is equal to 0°. Values are clockwise, so East = 90°.the value is between 0 and 180°(e.g 355° becomes 175°).';"
    
    
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.the_geom IS 'Geometry of the building. This geometry is normalized to avoid topological errors';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.pk IS 'Unique identifier for the buildings';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.PK_BLOCK IS 'Block identifier';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.perimeter IS 'Building perimeter';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.INSEE_INDIVIDUS IS 'Number of inhabitants derived from intersection of INSEE 200m gridded cells, taking into account the pai_nature (must be null), and the developped area (= area(building) * nb_niv)';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.hauteur IS ' Heigth of the building';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.hauteur_origin IS ' Approximate heigth of the building when the value hauteur is null';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.nb_niv IS 'Number of levels for a building based on the field hauteur';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.id_zone IS 'Unique identifier of a commune';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.pk_usr IS 'Unique identifier of the USR';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.area IS 'Area of the building';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.floor_area IS 'Sum of the building’s area for each level';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.vol IS 'Product of the area and the height';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.compacity_r IS 'Sum of external surfaces divided by the building’s volume power two-third';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.compacity_n IS 'Sum of free external surfaces divided by the building’s volume power two-third.';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.compactness IS 'The compactness ratio is defined as the ratio between the polygon’s length and the perimeter of a circle with the same area (Gravelius’s definition).';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.form_factor is 'Ratio between the polygon’s area and the square of the building’s perimeter.';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.concavity IS 'Ratio between the geometry’s area and its convex hull’s area';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.main_dir_deg IS 'The main direction corresponds to the direction (in degree) given by the longest side of the geometry’s minimum rectangle.The north is equal to 0°. Values are clockwise, so East = 90°. This value is ”modulo pi” expressed → the value is between 0 and 180° (e.g 355° becomes 175°).';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.b_floor_long IS 'Building perimeter';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.b_wall_area IS 'Total area of building’s walls (area of facade, including holes)';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.p_wall_long IS 'Total length of party walls';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.p_wall_area IS 'Total area of common walls (based on common hight of buildings)';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.nb_neighbor IS 'Number of party walls';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.free_p_wall_long IS 'Total length of free facades (= perimeter - total length of party walls)';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.free_ext_area IS 'Total area of free external facades';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.contiguity IS 'Ratio of total area of party walls divided by building’s facade area';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.p_vol_ratio IS 'Passiv volume ratio';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.fractal_dim IS 'The fractal dimension of the geometry is defined as 2log(Perimeter)/log(area) ';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.min_dist IS 'Minimum distance between one building and all the others that are in the USR';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.mean_dist IS 'Mean distance between one building and all the others that are in the USR';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.max_dist IS 'Maximum distance between one building and all the others that are in the USR';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.std_dist IS 'Standard deviation distance between one building and all the others that are in the USR';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.num_points IS 'Number of distinct points of the exterior ring of the building';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.l_tot IS 'Building’s perimeter of the exterior ring';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.l_cvx IS 'Perimeter of the convex hull of the building';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.l_3m IS 'Length of walls that are less than 3 meters from a road.';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.l_ratio IS 'Ratio between L_TOT and L_3M';"
    sql.execute "COMMENT ON COLUMN BUILDING_INDICATORS.l_ratio_cvx IS 'Ratio between L_3M and L_CVX';"
    
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.the_geom IS 'Geometry of the USR.';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.pk IS 'Unique identifier for the USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.INSEE_INDIVIDUS IS 'Number of inhabitants computed by sum of INSEE grid cells intersecting buildings of the USR, proportionaly to their floor area (nb_niv * area(building)) and only if the pai_nature of the building is null (which means residential a priori).)';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.INSEE_MENAGES IS 'Number of households having a permanent living computed by sum of INSEE grid cells intersecting buildings of the USR, proportionaly to their floor area (nb_niv * area(building)), and only if the pai_nature of the building is null (which means residential a priori).';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.INSEE_MEN_COLL IS 'Number of households living in collective housing computed by sum of INSEE grid cells intersecting buildings of the USR, proportionaly to their floor area (nb_niv * area(building)), and only if the pai_nature of the building is null (which means residential a priori).';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.INSEE_MEN_SURF IS 'Cumulated area of housings for households having a permanent living computed in square meter, by summing share of INSEE grid cells intersecting buildings of the islets, proportionaly to their developed area (nb_niv * area(building)), and only if the pai_nature of the building is null (which means residential a priori).';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.INSEE_SURFACE_COLLECTIF IS 'Estimation of collective housing from INSEE indicators: (=insee_men_coll/insee_menages*insee_men_surf)';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.VEGETATION_SURFACE IS 'Area of vegetation intersecting the USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.ROUTE_SURFACE IS 'Area of roads intersecting the USR. Computing is using the lenght of road segments intersecting the USR, and length of a clip of each segment with islet frontiers is multiplicated by a buffer having the road width divided by 2. Nota: roads with fictif to true have automatically a width of 0, and 65% of secondary roads with importance=5 and fictif=false have a null width. ';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.ROUTE_LONGUEUR IS 'Length of roads intersecting the USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.TROTTOIR_LONGUEUR IS 'Perimeter of the included USR made of union of contiguous parcels.';"
    
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.floor IS 'Sum of each building’s floor area';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.floor_ratio IS 'Ratio between the total floor area and the USR area';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.compac_mean_nw IS 'Non weighted buildings’s mean compacity in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.compac_mean_w IS 'Weighted buildings’s mean compacity in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.contig_mean IS 'Buildings’s mean contiguity in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.contig_std IS 'Buildings’s standard deviation contiguity in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.main_dir_std IS 'Buildings’s standard deviation main direction in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.h_mean IS 'Buildings’s mean height in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.h_std IS 'Buildings’s standard deviation height in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.p_vol_ratio_mean IS 'Buildings’s mean passiv volume ratio in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.b_area IS 'Total building’s area in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.b_vol IS 'Total building’s volume in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.b_vol_m IS 'Buildings’s mean volume in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.build_numb IS 'Buildings’s number in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.min_m_dist IS 'Mean value of the minimum distance between buildings in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.mean_m_dist IS 'Mean value of the mean distance between buildings in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.mean_std_dist IS 'Standard deviation of the mean distance between buildings in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.b_holes_area_mean IS 'Blocks’s mean courtyard ratio in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.b_std_h_mean IS 'Blocks’s mean standard deviation height in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.b_m_nw_compacity IS 'Block’s non weigthed mean compacity in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.b_m_w_compacity IS 'Block’s weigthed mean compacity in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.b_std_compacity IS 'Blocks’s standard deviation compacity in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.dist_to_center IS 'Distance between an USR (centroid) and its commune (centroid)';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.build_dens IS 'Buildings’s area density value';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.hydro_dens IS 'Hydrographic’s area density value';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.veget_dens IS 'Vegetation’s area density value';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.road_dens IS 'Road’s area density value';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.ext_env_area IS 'Total building’s external surface in an USR';"
    sql.execute "COMMENT ON COLUMN USR_INDICATORS.id_zone IS 'Unique identifier of a commune';"
    
    
    literalOutput = "All morphological indicators have calculated and stored in 3 tables USR_INDICATORS, BLOCK_INDICATORS and BUILDING_INDICATORS."

}


/** String output of the process. */
@LiteralDataOutput(
        title="Output message",
        resume="The output message")
String literalOutput
