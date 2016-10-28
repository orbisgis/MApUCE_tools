### Loading packages
library(randomForest)
library(RH2GIS)


### Loading the model based on the morphological train data
TUFA=get(load(model_path))

### Creating data to predict
dbSendQuery(con,"DROP TABLE IF EXISTS buildings_to_predict; CREATE TABLE buildings_to_predict AS SELECT a.PK,a.PK_USR, a.HAUTEUR_ORIGIN AS i_H_Origin,a.INSEE_INDIVIDUS AS i_INHAB, a.HAUTEUR AS i_H,a.NB_NIV AS i_LEVELS,a.AREA AS i_AREA,a.FLOOR_AREA AS i_FLOOR,a.VOL AS i_VOL,a.COMPACITY_R AS i_COMP_B,a.COMPACITY_N AS i_COMP_N,a.COMPACTNESS AS i_COMP,a.FORM_FACTOR AS i_FORM,a.CONCAVITY AS i_CONC, a.MAIN_DIR_DEG AS i_DIR,a.B_FLOOR_LONG AS i_PERI,a.B_WALL_AREA AS i_WALL_A,a.P_WALL_LONG AS i_PWALL_L,a.P_WALL_AREA AS i_PWALL_A,a.NB_NEIGHBOR AS i_Nb_NEI,a.FREE_P_WALL_LONG AS i_FWALL_L,a.FREE_EXT_AREA AS i_FREE_EXT_AREA,a.CONTIGUITY AS i_CONTIG,a.P_VOL_RATIO AS i_PASSIV_VOL,a.FRACTAL_DIM AS i_FRACTAL,a.MIN_DIST AS i_DIST_MIN,a.MEAN_DIST AS i_DIST_MEAN,a.MAX_DIST AS i_DIST_MAX,a.STD_DIST AS i_DIST_STD,a.NUM_POINTS AS i_Nb_POINTS,a.L_TOT AS i_L_TOT,a.L_CVX AS i_L_CVX,a.L_3M AS i_L_3M,b.AREA AS b_AREA,b.FLOOR_AREA AS b_FLOOR,b.VOL AS b_VOL,b.H_MEAN  AS b_H_MEAN,b.H_STD AS b_H_STD,b.COMPACITY AS b_COMP_N,b.HOLES_PERCENT AS b_HOLES_P,b.MAIN_DIR_DEG AS b_DIR,u.VEGETATION_SURFACE AS u_VEG_A,u.ROUTE_SURFACE AS u_ROAD_A,u.ROUTE_LONGUEUR AS u_ROAD_L,u.TROTTOIR_LONGUEUR AS u_SIDEWALK_L,u.INSEE_INDIVIDUS AS u_INHAB,u.INSEE_MENAGES AS u_HOUSE,u.INSEE_MEN_COLL AS u_COL_HOUSE,u.INSEE_MEN_SURF AS u_HOUSE_A,u.insee_surface_collectif AS u_COL_HOUSE_A, u.FLOOR AS u_FLOOR,u.FLOOR_RATIO AS u_COS,u.COMPAC_MEAN_NW AS u_COMP_NWMEAN,u.COMPAC_MEAN_W AS u_COMP_WMEAN,u.CONTIG_MEAN AS u_CONTIG_MEAN,u.CONTIG_STD AS u_CONTIG_STD,u.MAIN_DIR_STD AS u_DIR_STD,u.H_MEAN AS u_H_MEAN,u.H_STD AS u_H_STD,u.P_VOL_RATIO_MEAN AS u_PASSIV_VOL_MEAN,u.B_AREA AS u_AREA,u.B_VOL AS u_VOL,u.B_VOL_M AS u_VOL_MEAN,u.BUILD_NUMB AS u_Nb_BUILD,u.MIN_M_DIST AS u_DIST_MIN_MEAN,u.MEAN_M_DIST AS u_DIST_MEAN_MEAN,u.MEAN_STD_DIST AS u_DIST_MEAN_STD,u.B_STD_H_MEAN AS u_bH_STD_MEAN,u.B_M_NW_COMPACITY AS u_bCOMP_NWMEAN,u.B_M_W_COMPACITY AS u_bCOMP_WMEAN,u.B_STD_COMPACITY AS u_bCOMP_STD,u.DIST_TO_CENTER AS u_DIST_CENTER,u.BUILD_DENS AS u_BUILD_DENS,u.VEGET_DENS AS u_VEG_DENS,u.ROAD_DENS AS u_ROAD_DENS,u.EXT_ENV_AREA AS u_FWALL_A FROM BUILDING_INDICATORS AS A JOIN USR_INDICATORS AS U ON a.PK_USR = u.PK JOIN BLOCK_INDICATORS b ON a.PK_USR = b.PK_USR AND a.PK_BLOCK = b.PK_BLOCK;")


data_predict = dbGetQuery(con, "SELECT * FROM buildings_to_predict");


### Remove column identifiers
var_pred_com=data_predict[,-2]

### Compute typologies
set.seed(7)
typologie=predict(TUFA,var_pred_com,type="class")
### Matching typologies with building identifiers
tab_typo_bati=cbind.data.frame(data_predict[,1],typologie)
##Export typologies
dbWriteTable(con, "TMP_TYPO_BUILDINGS_MAPUCE", tab_typo_bati, append=TRUE, row.names=FALSE)

### Compute floor percent
pct_bati=data_predict$I_FLOOR/data_predict$U_FLOOR*100

### Compute % of typologies by USR
typo_USR=tapply(pct_bati,list(data_predict$PK_USR,typologie),sum)
typo_USR=ifelse(is.na(typo_USR),0.00,round(typo_USR,2))


### Extract first max and second max typologies
#Extract first max
extract_typo_maj=function(ligne)
{
val_maj=which.max(ligne)
return(colnames(typo_USR)[val_maj])
}

#Extract second max
extract_typo_sec=function(ligne)
{
val_sec=order(ligne)[length(ligne)-1]
return(ifelse(ligne[val_sec]==0,"na",colnames(typo_USR)[val_sec]))
}


##Combine first max typology
typo_majoritaire=apply(typo_USR,1,extract_typo_maj)

##Combine second max typology
typo_secondaire=apply(typo_USR,1,extract_typo_sec)

### Merging the two typologies at USR scale
tab_typo_usr=cbind(typo_USR,typo_majoritaire,typo_secondaire)
u_PK=rownames(tab_typo_usr)
tab_typo_usr=cbind.data.frame(u_PK,tab_typo_usr)

### Export typologies by USR
dbWriteTable(con, "TMP_TYPO_USR_MAPUCE", tab_typo_usr[,c(1,12,13)], append=TRUE, row.names=FALSE)

### Create final tables with geometries
dbSendQuery(con, "CREATE INDEX ON TMP_TYPO_BUILDINGS_MAPUCE(PK); CREATE TABLE TYPO_BUILDINGS_MAPUCE AS SELECT a.the_geom, b.pk as pk_building, a.pk_usr, a.id_zone, b.typo from BUILDINGS_MAPUCE a, TMP_TYPO_BUILDINGS_MAPUCE b where a.pk=b.pk; ")
dbSendQuery(con, "CREATE INDEX ON TMP_TYPO_USR_MAPUCE(PK_USR); CREATE TABLE TYPO_USR_MAPUCE AS SELECT a.the_geom, b.pk_usr, a.id_zone, b.typo_maj, b.typo_second  from USR_MAPUCE a, TMP_TYPO_USR_MAPUCE b where a.pk=b.pk_usr;")
dbSendQuery(con, "DROP TABLE IF EXISTS TMP_TYPO_BUILDINGS_MAPUCE, TMP_TYPO_USR_MAPUCE, buildings_to_predict;")