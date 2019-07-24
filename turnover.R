#install.packages('RODBC')
#install.packages('reshape2')
#install.packages('randomForest')
#install.packages('data.table')
#install.packages('DescTools')
library.data.table <- library(data.table)
library.DescTools <- library(DescTools)
library.Matrix <- library(Matrix)
library.RODBC <- library(RODBC)
library.reshape2 <- library(reshape2)
library.randomForest <- library(randomForest)
memory.size(max=TRUE)

dbhandle <- odbcDriverConnect('driver={SQL Server};server=104.130.165.170;database=Platform;uid=sa;pwd=Quantum!00')
uniqueOrgNums <- sqlQuery(dbhandle, 
	'select distinct
		OrganizationID 
	from platform..hireDate_vw hd 
		join platform..userattribute ua on hd.userid = ua.userid 
		join platform..users ui on hd.userid = ui.userid'
)

completeStor <- list()
for(i in 1:length(uniqueOrgNums$OrganizationID)){
print(i)
df <- sqlQuery(dbhandle, 
	paste("select hd.*,oa.Attribute+'_'+cast(oao.AttributeId as varchar)+'_'+Value+'_'+cast(oao.AttributeOptionId as varchar) as Value from platform..hireDate_vw hd  join platform..userattribute ua on hd.userid = ua.userid join platform..users ui on hd.userid = ui.userid join platform..OrganizationAttributeOption oao on ua.value = oao.OptionValue and ua.AttributeID = oao.AttributeId join platform..organizationattribute oa on oao.attributeid = oa.AttributeID where hd.OrganizationID ="
,
uniqueOrgNums$OrganizationID[i]
)
)

if(nrow(df) > 99){
iters <- 10
saveOutput <- list()

#Do some basic date formatting
df$HireDate <- as.Date(df$HireDate)
df$TermDate <- as.Date(ifelse(is.na(df$TermDate),'9999-12-31',as.character(df$TermDate)))

#Set some targets
df$term1 <- ifelse(df$TermDate - df$HireDate < 365,1,0)
df$term0.5 <- ifelse(df$TermDate - df$HireDate < 180,1,0)
df$term0.25 <- ifelse(df$TermDate - df$HireDate < 90,1,0)
df$keep <- ifelse(df$Value %like% '%@%',0,1)

howManyObsPerValue <- aggregate(UserID ~ Value,df,function(x){length(unique(x))})
keepValues <- howManyObsPerValue[howManyObsPerValue$UserID >= length(unique(df$UserID))*0.01,]
df <- df[df$keep == 1 & df$Value %in% keepValues$Value,]
dfTable <- as.data.table(df)

if(nrow(df) > 0){
widedfTable <- dcast(dfTable,term1 + term0.5 + term0.25 + UserID ~ Value,length)
widedf <- as.data.frame(widedfTable)

#Function to assign 1 to anything more than 1

just1 <- function(x){
	a <- ifelse(x > 0,1,0)
	return(a)
}


widedf[,4:ncol(widedf)] <- apply(widedf[,4:ncol(widedf)],2,just1)
colnames(widedf) <- paste('v',colnames(widedf),sep='')
colnames(widedf) <- gsub(pattern=' ',replacement='',colnames(widedf))
colnames(widedf) <- gsub(pattern='[[:punct:][_]]',replacement='',colnames(widedf))
colnames(widedf) <- gsub(pattern='[^[:alnum:][_] ]',replacement='',colnames(widedf))
colnames(widedf) <- gsub(pattern='/',replacement='',colnames(widedf))
df2 <- widedf[,c(1,5:ncol(widedf))]

if(sum(df2$vterm1) > 10){
startTime <- Sys.time()
importanceListFinal <- list()
modelResults <- list()

	for(j in 1:iters){
		gc()
		smp_size <- floor(0.8 * nrow(df2))
		train_ind <- sample(seq_len(nrow(df2)), size = smp_size)
		train <- df2[train_ind,]
	#up sample the turnovers in train

		trainUp <- train[train$vterm1 ==1,]
		numReplicates <- round(nrow(train)/nrow(trainUp))
		trainUpRep <- do.call(rbind, replicate(numReplicates, trainUp, simplify = FALSE))
		train <- rbind(train,trainUpRep)
		test <- df2[-train_ind, ]
		print(j)
		rf <- randomForest(as.factor(vterm1) ~ .,data=train,ntree=50)
		preds <- predict(rf,newdata=test)

		importanceList = data.frame(var = colnames(df2[,2:ncol(df2)]),importance(rf))
		importanceListFinal[[j]] <- importanceList
		modelResults[[j]] <- data.frame(j,table(prediction = preds,actual=test$vterm1))
	}
endTime <- Sys.time()
importanceListFinalUnlist <- do.call(rbind.data.frame,importanceListFinal)
avgVarImportance <- aggregate(MeanDecreaseGini ~ var,data=importanceListFinalUnlist,mean)
avgVarImportanceOrder <- avgVarImportance[order(-avgVarImportance$MeanDecreaseGini),]

modelResultsUnlist <- do.call(rbind.data.frame,modelResults)
modelResultsFinal <- aggregate(Freq ~ prediction + actual,data=modelResultsUnlist ,sum)
modelResultsFinal$AvgFreq <- modelResultsFinal$Freq/iters
completeStor[[i]] <- list(uniqueOrgNums$OrganizationID[i],avgVarImportanceOrder[1:20,],modelResultsFinal,startTime,endTime)
rm(list=setdiff(ls(), c('i','dbhandle','uniqueOrgNums','completeStor','library.data.table','library.DescTools','library.Matrix','library.RODBC','library.reshape2','library.randomForest')))
save(completeStor,file='C:/Users/BobKill/OneDrive - Quantum Workplace/Turnover/Calculate turnover/completeStorV3.RData')
gc()
}
}
}
}

save(completeStor,file='C:/Users/BobKill/OneDrive - Quantum Workplace/Turnover/Calculate turnover/completeStorV3.RData')


completeStor


