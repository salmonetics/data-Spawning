combineCAX <- function(f1,f2,f3,metafile){
  cax.adults <- read.csv(f1)
  cax.adults <- filter(cax.adults,POPID<500, RECOVERYDOMAIN!="Oregon Coast")
  pop11 <- filter(cax.adults,POPID==11,POPFIT=="Same")
  pop13 <- filter(cax.adults,POPID==13,SPAWNINGYEAR<=1997 | BESTVALUE=="Yes")
  pop45 <- filter(cax.adults,POPID==45,CONTACTAGENCY=="Washington Department of Fish and Wildlife")
  pop61 <- filter(cax.adults,POPID==61,POPFIT=="Same")
  #pop71 <- filter(cax.adults,POPID==71,CONTACTAGENCY=="Washington Department of Fish and Wildlife")
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
  #cax.adults <- filter(cax.adults,POPID != 71)
  #NF Salmon Steelhead records problematic: not just NF
  cax.adults <- filter(cax.adults,POPID != 98)
  
  cax.adults <- filter(cax.adults,POPID != 105)
  cax.adults <- filter(cax.adults,POPID != 107)
  cax.adults <- filter(cax.adults,POPID != 240)
  cax.adults <- filter(cax.adults,POPID != 241)
  cax.adults <- filter(cax.adults,POPID != 288)
  cax.adults <- filter(cax.adults,POPID != 302)
  
  cax.adults <- rbind(cax.adults,pop11,pop13,pop45,pop61,pop105,pop107,pop240,pop241,pop288,pop302)
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

pops <- read.csv("ca-data-all 02-08-2019 18 55_Populations.csv",stringsAsFactors=FALSE)


cax <- combineCAX("ca-data-all 10-29-2019 13 27_NOSA.csv","ca-data-all 10-29-2019 13 27_JuvOut.csv","ca-data-all 10-29-2019 13 27_PreSmolt.csv","CAX_metadata_11-01-2019.csv")
write.csv(cax,"CAX_data_11-01-2019.csv",row.names=FALSE)

#Create geomeans file
cax.1 <- select(cax,1,2,3,4,6,7,8,9,10,11)
names(cax.1) <- c("POPID","SPAWNINGYEAR","NOSAIJ","NOSAEJ","PHOSIJ","PHOSEJ","NOSJF","HOSJF","TSAIJ","TSAEJ")
combined <- merge(pops %>% select(POPID,NMFS_POPID),cax.1) %>% select(-1)


#Then calculate gmeans items and gmeans.all as per below, saving gmeans.all as:

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

gmeans.all <- merge(gmeans.NOSAEJ.10,gmeans.NOSAEJ.5,all=TRUE) %>% unique()
gmeans.all <- merge(gmeans.all,gmeans.NOSAIJ.10,all=TRUE) %>% unique()
gmeans.all <- merge(gmeans.all,gmeans.NOSAIJ.5,all=TRUE) %>% unique()
gmeans.all <- merge(gmeans.all,gmeans.TSAEJ.10,all=TRUE) %>% unique()
gmeans.all <- merge(gmeans.all,gmeans.TSAEJ.5,all=TRUE) %>% unique()
gmeans.all <- merge(gmeans.all,gmeans.TSAIJ.10,all=TRUE) %>% unique()
gmeans.all <- merge(gmeans.all,gmeans.TSAIJ.5,all=TRUE) %>% unique()

write.csv(gmeans.all,"CAX_geomeans_11-01-2019.csv",row.names=F)


