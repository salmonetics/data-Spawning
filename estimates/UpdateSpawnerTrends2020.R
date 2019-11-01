library(lubridate)
library(dplyr)
library(jsonlite)
library(mongolite)


#Start Mongo
#c:/Program Files/MongoDB/Server/3.4/bin/mongod.exe
#start MongoDB Compass
#Salmonetics DB

cax <- read.csv("ca-data-all 10-29-2019 13 27_NOSA.csv",stringsAsFactors=FALSE)
cax$VALIDATIONDATE <- mdy_hm(cax$VALIDATIONDATE)
cax$SNLOADDATE <- mdy_hm(cax$SNLOADDATE)
cax$DOWNLOADDATE <- mdy_hm(cax$DOWNLOADDATE)
cax$LASTUPDATED <- mdy_hm(cax$LASTUPDATED)
cax$UPDDATE <- mdy_hm(cax$UPDDATE)
cax <- arrange(cax,SPAWNINGYEAR,desc(LASTUPDATED))
cax$ext_datasource <- "CAX"

sps <- read.csv("SPS_MariCheck02012016.csv",stringsAsFactors=FALSE)
sps$ext_datasource <- "SPS"

m <- mongo(collection = "SpawnerAbundance_11-01-2019",  db = "salmonetics", url = "mongodb://localhost")

#pops <- summarise(group_by(cax,POPID))

pops <- read.csv("ca-data-all 02-08-2019 18 55_Populations.csv",stringsAsFactors=FALSE)
poplist <- list()

for(i in 1:nrow(pops)){
  list <- list()
  pop <- select(cax,POPID==pops$POPID[i])
  
}

npt <- read.csv("NPTSpawnerData_Update2-16-2018.csv",stringsAsFactors=FALSE)
npt$LastUpdated <- mdy(npt$LastUpdated)
npt$UpdDate <- mdy(npt$UpdDate)
names(npt) <- toupper(names(npt))
npt <- merge(select(pops,NMFS_POPID,POPID),npt)
npt$ext_datasource <- "NPT"


#new NPS ISEMP dataset
isemp_pops <- read.csv("ISEMP_Populations.csv",stringsAsFactors=FALSE)
npt <- read.csv("RK_IPTDS_escapement_4_19_19.csv",stringsAsFactors=FALSE) %>% filter(Median>0)
npt$TRT_POP <- npt$TRT.POP
npt <- select(npt,1,10,5,6,7,8) %>% merge(isemp_pops) %>% select(7,8,1,2,9,3,4,5,6)
names(npt) <- c("POPID","NMFS_POPID","Species","TRT_POP","Popname","SPAWNINGYEAR","NOSAIJ","NOSAIJLOWERLIMIT","NOSAIJUPPERLIMIT")
npt$NOSAIJALPHA <- 0.5
npt$ext_datasource <- "NPT"


isemp <- read.csv("AbundanceAddendum_07-18-2017.csv", stringsAsFactors=FALSE)
isemp <- select(isemp,PopID,Species,Year,Spawners,FracWild,Age2,Age3,Age4,Age5,Age6,notes)
names(isemp) <- c("NMFS_POPID","SPECIES","BROOD_YEAR","NUMBER_OF_SPAWNERS","FRACWILD","AGE_2_RETURNS","AGE_3_RETURNS","AGE_4_RETURNS","AGE_5_RETURNS","AGE_6_RETURNS","notes")
isemp$ext_datasource <- "ISEMP"


#this is Klickitat Steelhead
yak <- read.csv("Yakama.csv", stringsAsFactors=FALSE)
yak <- select(yak,1,2,4,5,11,17,23,29)
yak$POPID <- 61
yak$SPECIES <- "Steelhead"
yak$TSAIJ <- yak$Total.Estimate
yak$NOSAIJ <- yak$Wild.Estimate
yak$PHOSIJ <- yak$NOSAIJ/yak$TSAIJ
yak <- select(yak,1,2,10,9,11,12)
yak$ext_datasource <- "YAKAMA_Klickitat"

#This is Yakima MPG Steelhead
yak2 <- read.csv("YakimaMPGSteelhead_2-16-2018.csv", stringsAsFactors=FALSE)
yak2$SPECIES <- "Steelhead"
yak2$ext_datasource <- "YAKAMA_Steelhead"


getSPS <- function(df){
  vars <- c("NMFS_POPID","NUMBER_OF_SPAWNERS","BROOD_YEAR","FRACWILD","AGE_2_RETURNS","AGE_3_RETURNS","AGE_4_RETURNS","AGE_5_RETURNS","AGE_6_RETURNS","AGE_7_RETURNS")
  df <- filter(df,NUMBER_OF_SPAWNERS >= 0)
  for(i in 1:length(vars)){
    if(!vars[i]%in% names(df))df[vars[i]] <- NA
  }
  NMFS_POPID <- df$NMFS_POPID
  SPAWNINGYEAR <- df$BROOD_YEAR
  NOSAIJ <- ifelse(df$FRACWILD > -1, df$FRACWILD*df$NUMBER_OF_SPAWNERS, NA)
  NOSAEJ <- ifelse(df$SPECIES== "Chinook",ifelse(df$AGE_3_RETURNS>-1,(1-df$AGE_3_RETURNS)*NOSAIJ,NA),NA)
  PHOSIJ <- ifelse(df$FRACWILD > -1,1-df$FRACWILD, NA)
  PHOSEJ <- PHOSIJ
  NOSJF <- ifelse(df$SPECIES== "Chinook",ifelse(df$AGE_3_RETURNS>-1,df$AGE_3_RETURNS,NA),NA)
  HOSJF <- NOSJF
  TSAIJ <- df$NUMBER_OF_SPAWNERS
  TSAEJ <- ifelse(df$SPECIES== "Chinook",ifelse(df$AGE_3_RETURNS>-1,(1-df$AGE_3_RETURNS)*TSAIJ,NA),TSAIJ)
  AGE2PROP <- ifelse(df$AGE_2_RETURNS>=0,df$AGE_2_RETURNS,NA)
  AGE3PROP <- ifelse(df$AGE_3_RETURNS>=0,df$AGE_3_RETURNS,NA)
  AGE4PROP <- ifelse(df$AGE_4_RETURNS>=0,df$AGE_4_RETURNS,NA)
  AGE5PROP <- ifelse(df$AGE_5_RETURNS>=0,df$AGE_5_RETURNS,NA)
  AGE6PROP <- ifelse(df$AGE_6_RETURNS>=0,df$AGE_6_RETURNS,NA)
  AGE7PROP <- ifelse(df$AGE_7_RETURNS>=0,df$AGE_7_RETURNS,NA)
  ext_datasource <- df$ext_datasource
  
  df1 <- data.frame(NMFS_POPID=NMFS_POPID,SPAWNINGYEAR=SPAWNINGYEAR,NOSAIJ=NOSAIJ,NOSAEJ=NOSAEJ,PHOSIJ=PHOSIJ,PHOSEJ=PHOSEJ,NOSJF=NOSJF,HOSJF=HOSJF,TSAIJ=TSAIJ,TSAEJ=TSAEJ,AGE2PROP=AGE2PROP,AGE3PROP=AGE3PROP,AGE4PROP=AGE4PROP,AGE5PROP=AGE5PROP,AGE6PROP=AGE6PROP,AGE7PROP=AGE7PROP,ext_datasource=ext_datasource)  
  
  return(df1)
}

getCAX <- function(df){
  vars <- c("NMFS_POPID","SPAWNINGYEAR","NOSAIJ","NOSAEJ","PHOSIJ","PHOSEJ","NOSJF","HOSJF","TSAIJ","TSAEJ","AGE2PROP","AGE3PROP","AGE4PROP","AGE5PROP","AGE6PROP","AGE7PROP")
  for(i in 1:length(vars)){
    if(!vars[i]%in% names(df))df[vars[i]] <- NA
  }
  is.chk <- filter(pops,POPID==df$POPID[1])$SPECIES == "Chinook salmon"
  ext_datasource <- df$ext_datasource
  df1 <- select(df,NMFS_POPID,SPAWNINGYEAR,NOSAIJ,NOSAEJ,PHOSIJ,PHOSEJ,NOSJF,HOSJF,TSAIJ,TSAEJ,AGE2PROP,AGE3PROP,AGE4PROP,AGE5PROP,AGE6PROP,AGE7PROP,ext_datasource=ext_datasource)
  if(!is.chk){
    df1$NOSAIJ <- ifelse(is.na(df1$NOSAIJ),df1$NOSAEJ,df1$NOSAIJ)
    df1$TSAIJ <- ifelse(is.na(df1$TSAIJ),df1$TSAEJ,df1$TSAIJ)
    df1$PHOSIJ <- ifelse(is.na(df1$PHOSIJ),df1$PHOSEJ,df1$PHOSIJ)
  }
  else{
    df1$NOSAEJ <- ifelse(is.na(df1$NOSAEJ) & df1$AGE3PROP >= 0 & !is.na(df1$NOSAIJ),(1-df1$AGE3PROP)*df1$NOSAIJ,df1$NOSAEJ)
    df1$TSAEJ <- ifelse(is.na(df1$TSAEJ) & df1$AGE3PROP >= 0 & !is.na(df1$TSAIJ),(1-df1$AGE3PROP)*df1$TSAIJ,df1$TSAEJ)
  }
  return(df1)
}

getNPT <- function(df){
  #df$NOSAEJ <- ifelse(df$Species != "Chinook",df$NOSAIJ,NA)
  df$NOSAEJ <- df$NOSAIJ
  df$NOSAIJ <- ifelse(df$Species != "Chinook",df$NOSAIJ,NA)
  return(getCAX(df))
}

getKlickitat <- function(){
  
  
}

#best <- read.csv("SpawnerBestData.csv", stringsAsFactors=FALSE)
best <- read.csv("SpawnerBestData_11-01-2019.csv", stringsAsFactors=FALSE)
combined <- data.frame()
for(i in 1:nrow(best)){
  if(best$INCLUDE[i]==TRUE){
    temp <- m$find(query=best$QUERY[i])
    print(paste(best$QUERY[i], nrow(temp)))
    if(nrow(temp)>0){
      if(temp$ext_datasource[1]=="SPS" | temp$ext_datasource[1]=="ISEMP")combined <- rbind(combined,getSPS(temp))
      else if (temp$ext_datasource[1]=="NPT")combined <- rbind(combined,getNPT(temp))
      else combined <- rbind(combined,getCAX(temp))
      
    }
    #print(best$QUERY[i])    
  }
  
  rvars <- c("NOSAIJ","NOSAEJ","TSAIJ","TSAEJ")
  for(i in 1:length(rvars))combined[rvars[i]] <- round(combined[rvars[i]])
}


geomean<-function(x){
  iii<-(x>0)
  y<-exp(mean(log(x[iii]),na.rm=TRUE))
  return(round(y))
}

#modify functions to generalize for number of geomean years
getMeanData <- function(df,id,year,span){
  return(filter(df,NMFS_POPID==id & SPAWNINGYEAR > year-span & SPAWNINGYEAR <= year))
}

getGeoMeans <- function(df,span,min.years,name){
  geos <- data.frame()
  for(i in 1:nrow(df)){
    mdat <- getMeanData(df,df$NMFS_POPID[i],df$SPAWNINGYEAR[i],span)
    mdat <- mdat[!is.na(mdat[name]),]
    if(nrow(mdat) >= min.years){
      g <- geomean(mdat[name])
      geos <- rbind(geos,data.frame(NMFS_POPID=df$NMFS_POPID[i],SPAWNINGYEAR=df$SPAWNINGYEAR[i],Geomean=g))
    }
  }
  geos$Geomean <- ifelse(is.nan(geos$Geomean),NA,geos$Geomean)
  newname <- paste(name,"_",span,"yrGeomean",sep="")
  names(geos) <- c("NMFS_POPID","SPAWNINGYEAR",newname)
  return(geos)
}

getSingleYearGeo <- function(geo,year,name){
  geo <- filter(geo,SPAWNINGYEAR==year)
  geo <- data.frame(NMFS_POPID=geo$NMFS_POPID,geo=geo[name])
  names(geo) <- c("NMFS_POPID", paste(name,'_',year,sep=""))
  return(geo)
}

gmeans.NOSAEJ.10 <- getGeoMeans(combined,10,6,"NOSAEJ")
gmeans.NOSAEJ.5 <- getGeoMeans(combined,5,4,"NOSAEJ")
gmeans.NOSAIJ.10 <- getGeoMeans(combined,10,6,"NOSAIJ")
gmeans.NOSAIJ.5 <- getGeoMeans(combined,5,4,"NOSAIJ")
gmeans.TSAEJ.10 <- getGeoMeans(combined,10,6,"TSAEJ")
gmeans.TSAEJ.5 <- getGeoMeans(combined,5,4,"TSAEJ")
gmeans.TSAIJ.10 <- getGeoMeans(combined,10,6,"TSAIJ")
gmeans.TSAIJ.5 <- getGeoMeans(combined,5,4,"TSAIJ")

gmeans.all <- merge(gmeans.NOSAEJ.10,gmeans.NOSAEJ.5,all=TRUE)
gmeans.all <- merge(gmeans.all,gmeans.NOSAIJ.10,all=TRUE)
gmeans.all <- merge(gmeans.all,gmeans.NOSAIJ.5,all=TRUE)
gmeans.all <- merge(gmeans.all,gmeans.TSAEJ.10,all=TRUE)
gmeans.all <- merge(gmeans.all,gmeans.TSAEJ.5,all=TRUE)
gmeans.all <- merge(gmeans.all,gmeans.TSAIJ.10,all=TRUE)
gmeans.all <- merge(gmeans.all,gmeans.TSAIJ.5,all=TRUE)

combi <- merge(combined,gmeans.all)
#write.csv(combi,"combined_abundance_geomeans_1-22-2018.csv",row.names=FALSE)
#write.csv(combi,"combined_abundance_geomeans_3-04-2019.csv",row.names=FALSE)
#write.csv(combi,"combined_abundance_geomeans_3-25-2019.csv",row.names=FALSE)
#write.csv(combi,"combined_abundance_geomeans_3-27-2019.csv",row.names=FALSE)
#write.csv(combi,"combined_abundance_geomeans_4-19-2019.csv",row.names=FALSE)
write.csv(combi,"combined_abundance_geomeans_11-01-2019.csv",row.names=FALSE)
#get json page for each pop-year
#first, get ids of those used, then loop through those

getSPS.id <- function(df){
  vars <- c("NMFS_POPID","NUMBER_OF_SPAWNERS","BROOD_YEAR","FRACWILD","AGE_2_RETURNS","AGE_3_RETURNS","AGE_4_RETURNS","AGE_5_RETURNS","AGE_6_RETURNS","AGE_7_RETURNS")
  df <- filter(df,NUMBER_OF_SPAWNERS >= 0)
  for(i in 1:length(vars)){
    if(!vars[i]%in% names(df))df[vars[i]] <- NA
  }
  MONGOID <- df$MONGOID
  NMFS_POPID <- df$NMFS_POPID
  SPAWNINGYEAR <- df$BROOD_YEAR
  NOSAIJ <- ifelse(df$FRACWILD > -1, df$FRACWILD*df$NUMBER_OF_SPAWNERS, NA)
  NOSAEJ <- ifelse(df$SPECIES== "Chinook",ifelse(df$AGE_3_RETURNS>-1,(1-df$AGE_3_RETURNS)*NOSAIJ,NA),NA)
  PHOSIJ <- ifelse(df$FRACWILD > -1,1-df$FRACWILD, NA)
  PHOSEJ <- PHOSIJ
  NOSJF <- ifelse(df$SPECIES== "Chinook",ifelse(df$AGE_3_RETURNS>-1,df$AGE_3_RETURNS,NA),NA)
  HOSJF <- NOSJF
  TSAIJ <- df$NUMBER_OF_SPAWNERS
  TSAEJ <- ifelse(df$SPECIES== "Chinook",ifelse(df$AGE_3_RETURNS>-1,(1-df$AGE_3_RETURNS)*TSAIJ,NA),TSAIJ)
  AGE2PROP <- ifelse(df$AGE_2_RETURNS>=0,df$AGE_2_RETURNS,NA)
  AGE3PROP <- ifelse(df$AGE_3_RETURNS>=0,df$AGE_3_RETURNS,NA)
  AGE4PROP <- ifelse(df$AGE_4_RETURNS>=0,df$AGE_4_RETURNS,NA)
  AGE5PROP <- ifelse(df$AGE_5_RETURNS>=0,df$AGE_5_RETURNS,NA)
  AGE6PROP <- ifelse(df$AGE_6_RETURNS>=0,df$AGE_6_RETURNS,NA)
  AGE7PROP <- ifelse(df$AGE_7_RETURNS>=0,df$AGE_7_RETURNS,NA)
  ext_datasource <- df$ext_datasource
  
  df1 <- data.frame(MONGOID=MONGOID,NMFS_POPID=NMFS_POPID,SPAWNINGYEAR=SPAWNINGYEAR,NOSAIJ=NOSAIJ,NOSAEJ=NOSAEJ,PHOSIJ=PHOSIJ,PHOSEJ=PHOSEJ,NOSJF=NOSJF,HOSJF=HOSJF,TSAIJ=TSAIJ,TSAEJ=TSAEJ,AGE2PROP=AGE2PROP,AGE3PROP=AGE3PROP,AGE4PROP=AGE4PROP,AGE5PROP=AGE5PROP,AGE6PROP=AGE6PROP,AGE7PROP=AGE7PROP,ext_datasource=ext_datasource)  
  
  return(df1)
}

getCAX.id <- function(df){
  vars <- c("NMFS_POPID","SPAWNINGYEAR","NOSAIJ","NOSAEJ","PHOSIJ","PHOSEJ","NOSJF","HOSJF","TSAIJ","TSAEJ","AGE2PROP","AGE3PROP","AGE4PROP","AGE5PROP","AGE6PROP","AGE7PROP")
  for(i in 1:length(vars)){
    if(!vars[i]%in% names(df))df[vars[i]] <- NA
  }
  is.chk <- filter(pops,POPID==df$POPID[1])$SPECIES == "Chinook salmon"
  ext_datasource <- df$ext_datasource
  df1 <- select(df,MONGOID,NMFS_POPID,SPAWNINGYEAR,NOSAIJ,NOSAEJ,PHOSIJ,PHOSEJ,NOSJF,HOSJF,TSAIJ,TSAEJ,AGE2PROP,AGE3PROP,AGE4PROP,AGE5PROP,AGE6PROP,AGE7PROP,ext_datasource=ext_datasource)
  if(!is.chk){
    df1$NOSAIJ <- ifelse(is.na(df1$NOSAIJ),df1$NOSAEJ,df1$NOSAIJ)
    df1$TSAIJ <- ifelse(is.na(df1$TSAIJ),df1$TSAEJ,df1$TSAIJ)
    df1$PHOSIJ <- ifelse(is.na(df1$PHOSIJ),df1$PHOSEJ,df1$PHOSIJ)
  }
  return(df1)
}

combined.id <- data.frame()
for(i in 1:nrow(best)){
  if(best$INCLUDE[i]==TRUE){
    temp <- m$find(query=best$QUERY[i])
    temp1 <- m$find(query=best$QUERY[i],fields = '{"_id": true}')
    print(paste(best$QUERY[i], nrow(temp)))
    if(nrow(temp)>0){
      names(temp1) <- c("MONGOID")
      temp <- cbind(temp1,temp)      
      if(temp$ext_datasource[1]=="SPS" | temp$ext_datasource[1]=="ISEMP")combined.id <- rbind(combined.id,getSPS.id(temp))
      else combined.id <- rbind(combined.id,getCAX.id(temp))
      
    }
    #print(best$QUERY[i])    
  }
}

combi.id <- merge(combined.id,gmeans.all,all.x=TRUE)

getAllData <- function(id,popid,year){
  query1 <- paste('{"_id": {"$oid": "',id,'"}}',sep="")
  query2 <- paste('{"_id": {"$ne": {"$oid": "',id,'"}}, "NMFS_POPID": ',popid, ', "$or": [{"SPAWNINGYEAR": ',year,'},{"BROOD_YEAR": ',year, '}]}',sep="")
  cat(query1)
  cat(query2)
  #default <- m$find(query1)
  it <- m$iterate(query=query1)
  
  dlist <- list()
  dlist[[1]] <- it$one()
  
  it <- m$iterate(query=query2)
  
  i <- 2
  while(!is.null(x <- it$one())){
    dlist[[i]] <- x
    i <- i + 1
  }
  return(dlist)
}

for(i in 1:nrow(combi.id)){
  #for(i in 1:10){
  if(combi.id$SPAWNINGYEAR[i]>1989){
    dlist <- getAllData(combi.id$MONGOID[i],combi.id$NMFS_POPID[i],combi.id$SPAWNINGYEAR[i])
    fname <- paste("SpawnerAbundanceData/POP",combi.id$NMFS_POPID[i],"YEAR",combi.id$SPAWNINGYEAR[i],".json",sep="")
    out <- writeLines(toJSON(dlist,pretty=TRUE,auto_unbox=TRUE),fname)
  }
}

#get current best CAX data
caxbest <- read.csv("CAX_BestData_04-16-2019.csv", stringsAsFactors=FALSE)
bestcax <- data.frame()
for(i in 1:nrow(caxbest)){
  if(caxbest$INCLUDE[i]==TRUE){
    temp <- m$find(query=caxbest$QUERY[i])
    print(paste(caxbest$QUERY[i], nrow(temp)))
    if(nrow(temp)>0){
      bestcax <- rbind(bestcax,getCAX(temp))
    }
    #print(best$QUERY[i])    
  }
}
rvars <- c("NOSAIJ","NOSAEJ","TSAIJ","TSAEJ")
for(i in 1:length(rvars))bestcax[rvars[i]] <- round(bestcax[rvars[i]])

combineCAX <- function(f1,f2,f3,metafile){
  cax.adults <- read.csv(f1)
  cax.adults <- filter(cax.adults,POPID<500, RECOVERYDOMAIN!="Oregon Coast")
  pop11 <- filter(cax.adults,POPID==11,POPFIT=="Same")
  pop13 <- filter(cax.adults,POPID==13,SPAWNINGYEAR<=1997 | BESTVALUE=="Yes")
  pop45 <- filter(cax.adults,POPID==45,CONTACTAGENCY=="Washington Department of Fish and Wildlife")
  pop61 <- filter(cax.adults,POPID==61,POPFIT=="Same")
  pop71 <- filter(cax.adults,POPID==71,CONTACTAGENCY=="Washington Department of Fish and Wildlife")
  pop105 <- filter(cax.adults,POPID==105,TRTMETHOD=="Yes")
  pop107 <- filter(cax.adults,POPID==107,CONTACTAGENCY=="Washington Department of Fish and Wildlife")
  pop240 <- filter(cax.adults,POPID==240,BESTVALUE=="Yes")
  pop241 <- filter(cax.adults,POPID==241,BESTVALUE=="Yes")
  pop288 <- filter(cax.adults,POPID==288,BESTVALUE=="Yes")
  pop302 <- filter(cax.adults,POPID==302,BESTVALUE=="Yes")
  
  #SF Clearwater Upper (Dry)
  cax.adults <- filter(cax.adults,POPID != 5)
  cax.adults <- filter(cax.adults,POPID != 13)
  cax.adults <- filter(cax.adults,POPID != 45)
  cax.adults <- filter(cax.adults,POPID != 61)
  cax.adults <- filter(cax.adults,POPID != 71)
  #NF Salmon Steelhead records problematic: not just NF
  cax.adults <- filter(cax.adults,POPID != 98)
  
  cax.adults <- filter(cax.adults,POPID != 105)
  cax.adults <- filter(cax.adults,POPID != 107)
  cax.adults <- filter(cax.adults,POPID != 240)
  cax.adults <- filter(cax.adults,POPID != 241)
  cax.adults <- filter(cax.adults,POPID != 288)
  cax.adults <- filter(cax.adults,POPID != 302)
  
  cax.adults <- rbind(cax.adults,pop11,pop13,pop45,pop61,pop71,pop105,pop107,pop240,pop241,pop288,pop302)
  #meta <- cax.adults[,78:94]
  meta <- select(cax.adults,POPFIT,POPFITNOTES,TRTMETHOD,CONTACTAGENCY,METHODNUMBER,PROTMETHNAME,PROTMETHURL,PROTMETHDOCUMENTATION,METHODADJUSTMENTS,COMMENTS,NULLRECORD,DATASTATUS,LASTUPDATED,INDICATORLOCATION,METRICLOCATION,MEASURELOCATION,CONTACTPERSONFIRST,CONTACTPERSONLAST,CONTACTPHONE,CONTACTEMAIL,METACOMMENTS,SUBMITAGENCY,LASTMODIFIEDDATE)
  meta1 <- unique(meta)
  meta1$METAID <- seq(1:nrow(meta1))
  write.csv(meta1,metafile,row.names=FALSE)
  cax.adults <- merge(cax.adults,meta1)
  cax.adults <- select(cax.adults,POPID,SPAWNINGYEAR,NOSAIJ,NOSAEJ,NOBROODSTOCKREMOVED,PHOSIJ,PHOSEJ,NOSJF,HOSJF,TSAIJ,TSAEJ,AGE2PROP,AGE3PROP,AGE4PROP,AGE5PROP,AGE6PROP,AGE7PROP,METAID)
  cax.adults$YEAR <- cax.adults$SPAWNINGYEAR
  cax.adults$SPAWNINGYEAR <- NULL
  #write.csv(cax.adults,"CAX_data.csv",row.names=FALSE)
  #cax.adults <- select(cax.adults,7,13,15,17,21,25,26,30,34,38,39,43,47,50,53,56,59,62)
  
  cax.juvs <- read.csv(f2)
  cax.juvs <- filter(cax.juvs,POPID<500, RECOVERYDOMAIN!="Oregon Coast")
  juvs30 <- filter(cax.juvs,POPID==30,BESTVALUE=="Yes")
  juvs95 <- filter(cax.juvs,POPID==95,BESTVALUE=="Yes")
  #Lochsa (Wet)
  cax.juvs <- filter(cax.juvs,POPID != 38)  
  cax.juvs <- filter(cax.juvs,POPID != 30)
  #Lower Clearwater Steelhead
  cax.juvs <- filter(cax.juvs,POPID != 77)
  cax.juvs <- filter(cax.juvs,POPID != 95)
  #NF Salmon Steelhead portion only
  cax.juvs <- filter(cax.juvs,POPID != 98)
  #Lower Gorge Winter Steelhead portion only
  cax.juvs <- filter(cax.juvs,POPID != 312)  
  cax.juvs <- rbind(cax.juvs,juvs30,juvs95)
  
  cax.pre <- read.csv(f3)
  cax.pre <- filter(cax.pre,POPID<500, RECOVERYDOMAIN!="Oregon Coast")
  cax.filter <- filter(cax.pre,POPID != 107)
  
  cax.juvs <- select(cax.juvs,POPID,POPFIT,POPFITNOTES,OUTMIGRATIONYEAR,TOTALNATURAL,AGE0PROP,AGE1PROP,AGE2PROP,AGE3PROP,AGE4PLUSPROP)
  names(cax.juvs) <- c("POPID","JPOPFIT","JPOPFITNOTES","YEAR","JTOTALNATURAL","JAGE0PROP","JAGE1PROP","JAGE2PROP","JAGE3PROP","JAGE4PLUSPROP")
  cax.pre <- select(cax.pre,POPID,POPFIT,POPFITNOTES,SURVEYYEAR,ABUNDANCE,AGE0PROP,AGE1PROP,AGE3PROP)
  names(cax.pre) <- c("POPID","PPOPFIT","PPOPFITNOTES","YEAR","PABUNDANCE","PAGE0PROP","PAGE1PROP","PAGE2PROP")
  
  cax <- merge(cax.adults,cax.juvs,all=TRUE)
  cax <- merge(cax,cax.pre,all=TRUE)
  return(cax)
}