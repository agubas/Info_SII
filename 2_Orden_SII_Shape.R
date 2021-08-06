##### Datos al Shape #####
library(rgdal)
library(raster)
library(foreach)
library(beepr)
library(doParallel)
library(xlsx)
library(readxl)
gCentroidWithin <- function(pol) {
  require(rgeos)
  
  pol$.tmpID <- 1:length(pol)
  # initially create centroid points with gCentroid
  initialCents <- gCentroid(pol, byid = T)
  
  # add data of the polygons to the centroids
  centsDF <- SpatialPointsDataFrame(initialCents, pol@data)
  centsDF$isCentroid <- TRUE
  
  # check whether the centroids are actually INSIDE their polygon
  centsInOwnPoly <- sapply(1:length(pol), function(x) {
    gIntersects(pol[x,], centsDF[x, ])
  })
  # substitue outside centroids with points INSIDE the polygon
  newPoints <- SpatialPointsDataFrame(gPointOnSurface(pol[!centsInOwnPoly, ], 
                                                      byid = T), 
                                      pol@data[!centsInOwnPoly,])
  newPoints$isCentroid <- FALSE
  centsDF <- rbind(centsDF[centsInOwnPoly,], newPoints)
  
  # order the points like their polygon counterpart based on `.tmpID`
  centsDF <- centsDF[order(centsDF$.tmpID),]
  
  # remove `.tmpID` column
  centsDF@data <- centsDF@data[, - which(names(centsDF@data) == ".tmpID")]
  
  cat(paste(length(pol), "polygons;", sum(centsInOwnPoly), "actual centroids;", 
            sum(!centsInOwnPoly), "Points corrected \n"))
  
  return(centsDF)
}
options(scipen=999)
##### Carga de Datos #####
setwd("//SVWIN022/00.cit/05.INVESTIGACION/03. FONDAP/5. PROYECTOS COES - CIT/09. Indicadores_MFA/03. BASE DE DATOS")
# noms=as.data.frame(read_excel("Nombres_Comunas.xlsx",sheet = "Hoja1"))
# saveRDS(noms,"Nombres_Comunas.rds")
noms=readRDS("Nombres_Comunas.rds")
noms=lapply(1:nrow(noms), function(y){
  cont=1
  x=noms[y,]
  out=NULL
  while (nchar(x)>0) {
    if(cont>0){
      rem=trimws(sub('[A-Z].*', '', x))
    }else{
      rem=trimws(sub('[0-9].*', '', x))
    }
    cont=cont*(-1)
    if(exists("out")){out=c(out,rem)}else{out=rem}
    x=trimws(gsub(rem,"",x))
  }
  return(out) 
})
noms <- suppressWarnings(data.frame(matrix(unlist(noms), nrow=length(noms), byrow=T),stringsAsFactors=FALSE))
names(noms)=c("codigo","comuna","codigo","comuna","codigo","comuna","codigo","comuna")
noms = rbind(noms[,1:2],noms[,3:4],noms[,5:6],noms[,7:8])
noms$codigo=as.numeric(noms$codigo)
noms=noms[!duplicated(noms$codigo),]
amebas=readOGR("Shapes","Área_urbana_consolidada_2016_09202017",stringsAsFactors = F)
amebas=subset(amebas,select="NOMBRE")
amebas=spTransform(amebas,CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0 "))
manzanas=readOGR("Shapes","Nivel_Comercio",stringsAsFactors = F)
##### Orden Gemetria y Calculo de Areas
manzanas$area=area(manzanas)
cl=makeCluster(6,type="SOCK")
registerDoParallel(cl) 
mz=unique(manzanas$manzana)[1]
areas_rep=foreach(mz = unique(manzanas$manzana),.combine="rbind") %dopar% {
  suma=sum(manzanas@data[manzanas$manzana==mz,"area"],na.rm=T)
  suma
}
registerDoSEQ()
areas_rep=as.data.frame(areas_rep)
areas_rep$manzana=unique(manzanas$manzana)
names(areas_rep)=c("areas","manzana")
manzanas=manzanas[,c("COD_CIT","reg","c_com","manz","n_com","manzana","area")]
manzanas=sp::merge(manzanas,areas_rep,by="manzana")
##### Seleccion Area Urbana Consolidada #####
manzanas_p=gCentroidWithin(manzanas)#SpatialPointsDataFrame(manzanas,manzanas@data,proj4string = CRS(proj4string(manzanas)))
manzanas$am=unlist(over(manzanas_p,amebas ))
manzanas=manzanas[!is.na(manzanas$am),]
# Guardar Comunas y ciudades que tenemos considerando
considerado = manzanas@data[,c("am","c_com")]
considerado = considerado[!duplicated(considerado),]
names(considerado) = c("ciudad","codigo_comuna")
names(noms) = c("codigo_comuna","nombre_comuna")
considerado = merge(considerado , noms,by="codigo_comuna",all.x=T)
write.xlsx(considerado,"Urbes_Consideradas.xlsx",sheetName = "Urbes",append = F,row.names = F)
##### Iterador Adjuntar Info  Shape #####
## Vectores Iterador
regiones=1:13
anos=c("2015_1","2015_2","2016_1","2016_2","2017_1","2017_2","2018_1","2018_2","2019_1")
validos=c("M_A","M_B","M_C","M_D","M_E","M_F","M_G","M_H","M_I","M_L","M_M",
          "M_O","M_P","M_Q","M_S","M_T","M_V","M_W","M_Y","M_Z","manzana")
reg=1
an=anos[9]
for (reg in regiones) {
  manz_reg=manzanas[manzanas$reg==reg,]
  for(an in anos){
    ## Carga y Orden de Datos
    predios=readRDS(paste0("SII/N_",an,"_r",reg,".rds"))
    predios$manzana=paste(predios$comuna , predios$manzana,sep="-")
    suelo=aggregate(superficie_terreno ~ manzana,data=predios,FUN=sum)
    lineas=readRDS(paste0("SII/NL_",an,"_r",reg,".rds"))
    lineas$manzana=paste(lineas$comuna , lineas$manzana,sep="-")
    names(lineas) = c("comuna","manzana","predial","linea","material","calidad","ano","supc","uso","condicion","region","reg" )
    ## Suma numero de metros por uso por manzana (Intensivo en RAM)
    constru=as.data.frame.matrix(table(rep(lineas$manzana,times=lineas$supc),rep(lineas$uso,times=lineas$supc)))
    gc() # Limpia RAM
    constru$manzana=as.character(row.names(constru)) # Recupera id de manzana
    names(constru) = c(paste0("M_",names(constru)[-ncol(constru)]) , "manzana") # Nuevos Nombres
    constru=constru[,names(constru) %in% validos] # Solo nos quedamos con los usos normados, agregamos cuando faltan
    faltan=validos[!validos %in% names(constru)]    
    for (fal in faltan) {
      constru[,fal]=NA
    }
    constru[is.na(constru)]=0
    suelo[is.na(suelo)]=0
    ## Nuevos nombres para que incluyan aÃ±o
    constru=constru[,c("manzana",validos[-length(validos)])]
    nuevos_nom=c(validos[-length(validos)])
    nuevos_nom=paste(nuevos_nom,rep(an,length(nuevos_nom)),sep="_")
    names(constru)=c("manzana",nuevos_nom)
    nuevo_nom_sup=paste(an,"sup_terr",sep="_")
    names(suelo) = c("manzana",nuevo_nom_sup)
    ## Pegar Info al shape
    manz_reg=sp::merge(manz_reg,constru,by="manzana")
    manz_reg=sp::merge(manz_reg,suelo,by="manzana")
    ## Prorrata construccion en manzanas repetidas segun area
    nuevos_nom=c(nuevos_nom,nuevo_nom_sup)
    manz_reg@data[,nuevos_nom][is.na(manz_reg@data[,nuevos_nom])]=0
    manz_reg@data[,nuevos_nom]=manz_reg@data[,nuevos_nom]*(manz_reg$area/manz_reg$areas)
    print(an)
  }
  saveRDS(manz_reg,paste0("SII/","metros_",reg,".rds"))
  writeOGR(manz_reg,"Shapes/Metros_Uso",paste0("metros_",reg),driver="ESRI Shapefile",overwrite_layer=T)
  print(reg)
  beep(2)
}
beep(5)

reg=13
saveRDS(manz_reg,paste0("SII/","metros_",reg,".rds"))
writeOGR(manz_reg,"Shapes/Metros_Uso",paste0("metros_",reg),driver="ESRI Shapefile",overwrite_layer=T)
