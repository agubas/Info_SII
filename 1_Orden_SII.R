##### Orden Datos SII #####
setwd("C:\Users\o\Documents\GitHub\Info_SII\SII")
library(readr)
##### Carga de Datos y Orden #####
anos=c("2015_1","2015_2","2016_1","2016_2","2017_1","2017_2","2018_1","2018_2","2019_1")
an=anos[9]
for (an in anos) {
  
N <- read_delim(paste0("txt/BRORGA2441N_NAC_",an,".txt"), 
                                     "|", escape_double = FALSE, col_names = FALSE, 
                                     trim_ws = TRUE)
NL <- read_delim(paste0("txt/BRORGA2441NL_NAC_",an,".txt"), 
                "|", escape_double = FALSE, col_names = FALSE, 
                trim_ws = TRUE)
## N
             "predio_comun_1","comuna_comun_2","manzana_comun_2","predio_comun_2","superficie_terreno")
attr(N,"spec")=NULL
N$comuna=as.numeric(N$comuna)
N$manzana=as.numeric(N$manzana)
N$predio=as.numeric(N$predio)
N$avaluo=as.numeric(N$avaluo)
N$contribucion_sem=as.numeric(N$contribucion_sem)
N$exento=as.numeric(N$exento)
N$comuna_comun_1=as.numeric(N$comuna_comun_1)
N$manzana_comun_1=as.numeric(N$manzana_comun_1)
N$predio_comun_1=as.numeric(N$predio_comun_1)
N$comuna_comun_2=as.numeric(N$comuna_comun_2)
N$manzana_comun_2=as.numeric(N$manzana_comun_2)
N$predio_comun_2=as.numeric(N$predio_comun_2)
N$superficie_terreno=as.numeric(N$superficie_terreno)
N$reg=N$region=floor(N$comuna/1000)
N$reg[N$reg>13]=13
## NL
names(NL) = c("comuna","manzana","predial","linea","material","calidad","ano","superficie_construccion","uso","condicion")
attr(NL,"spec")=NULL
NL$comuna=as.numeric(NL$comuna)
NL$manzana=as.numeric(NL$manzana)
NL$predial=as.numeric(NL$predial)
NL$linea=as.numeric(NL$linea)
NL$calidad=as.numeric(NL$calidad)
NL$ano=as.numeric(NL$ano)
NL$superficie_construccion=as.numeric(NL$superficie_construccion)
NL$reg=NL$region=floor(NL$comuna/1000)
NL$reg[NL$reg>13]=13
for (reg in 1:13) {
  N_reg=N[N$reg==reg,]
  NL_reg=NL[NL$reg==reg,]
  saveRDS(N_reg,paste0("N_",an,"_r",reg,".rds"))
  saveRDS(NL_reg,paste0("NL_",an,"_r",reg,".rds"))
  print(reg)
}
saveRDS(N,paste0("N_",an,".rds"))
saveRDS(NL,paste0("NL_",an,".rds"))
print(an)
}
##### Compilacion Bases para Conteo de Metros #####
# Se genera un base que comipel todos los años de manera de evitar el "sesgo de subestimacion en años pasados" para
# los años 2015,2016,2017,2018 pero es poco robusto al final, la base no esta lo suficientemente bien compialda con los años de constr. 
anos=c("2015_1","2015_2","2016_1","2016_2","2017_1","2017_2","2018_1","2018_2","2019_1")
regiones=1:13
an=anos[1]
reg=11
for (reg in regiones) {
  setwd("C:\Users\o\Documents\GitHub\Info_SII")
  lineas=readRDS(paste0("SII/NL_",anos[length(anos)],"_r",reg,".rds"))
  lineas$id=paste(lineas$comuna,lineas$manzana,lineas$predial,lineas$linea,lineas$ano,sep="-")
  lineas_n=data.frame()
  an=anos[length(anos)-1]    
  for (an in rev(anos[-length(anos)])) {
    lineas_rep=readRDS(paste0("SII/NL_",an,"_r",reg,".rds"))
    lineas_rep$id=paste(lineas_rep$comuna,lineas_rep$manzana,lineas_rep$predial,lineas_rep$linea,lineas_rep$ano,sep="-")
    nuevas=lineas_rep[!lineas_rep$id %in% lineas$id,]
    lineas_n=rbind(lineas_n,nuevas) 
  }
  lineas=rbind(lineas,lineas_n)
  saveRDS(lineas,paste0("SII/NL_comp_r",reg,".rds"))
  print(reg)
  beepr::beep(2)
}
beepr::beep(5)
