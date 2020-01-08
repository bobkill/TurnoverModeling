library.data.table <- require(data.table)
library.DescTools <- require(DescTools)
library.Matrix <- require(Matrix)
library.RODBC <- require(RODBC)
library.reshape2 <- require(reshape2)
library.randomForest <- require(randomForest)

'driver={SQL Server};server=104.130.165.170;database=Platform;uid=sa;pwd=Quantum!00'
myOrgNums <- sqlQuery(dbhandle, 
	'select distinct
		OrganizationID 
	from platform.dbo.DataMLRK2'
)



clientSpecificModeler(
dbServer='104.130.165.170',
db='Platform',
dbUsername='sa',
dbPassword='Quantum!00',
listOrgNums = myOrgNums,
initialRecordsRequired = 99,
trees = 150
#proportionRecordsPerVariableRequired= 0.05
)



clientSpecificModeler <- function(dbServer,db,dbUsername,dbPassword,listOrgNums,initialRecordsRequired,trees)
{

odbcConnectString <- paste('driver={SQL Server}',';server=',dbServer,';database=',db,';uid=',dbUsername,';pwd=',dbPassword,sep='')
dbhandle <- odbcDriverConnect(odbcConnectString)

uniqueOrgNums <- listOrgNums

completeStor <- list()
for(i in 1:length(uniqueOrgNums$OrganizationID)){
	print(i)
	dfSurvey <- sqlQuery(dbhandle, 
		paste("select hd.UserID,hd.HireDate,hd.TermDate,DateCreated,cycledate,questionid,surveyRankID,cast(sd.questionid as varchar) + 'BOBSEPARATEHERE' + response as Value from platform..userinfo hd join platform.dbo.DataMLRK2 sd on sd.platformuserid = hd.userid where sd.OrganizationID ="
	,
	uniqueOrgNums$OrganizationID[i]
	,' and cycledate between hiredate and termdate or (organizationid = '
	,uniqueOrgNums$OrganizationID[i]
	,'and cycledate > hiredate and termdate is null)'
	)
	)

	if(exists('dfSurvey')){

		if(nrow(dfSurvey) > 0){

			uniqueOrgSurveyID2 <- unique(dfSurvey$surveyRankID)#There are probably missing users in attributes
			uniqueOrgSurveyID <- uniqueOrgSurveyID2[order(uniqueOrgSurveyID2)]
			uniqueOrgSurveyID <- uniqueOrgSurveyID[length(uniqueOrgSurveyID)]

			df <- dfSurvey[dfSurvey$surveyRankID <= uniqueOrgSurveyID,]

			if(is.data.frame(df)){

				if(nrow(df) > initialRecordsRequired){

					#Do some basic date formatting
					df$HireDate <- as.Date(df$HireDate)
					df$TermDate <- as.Date(ifelse(is.na(df$TermDate),'9999-12-31',as.character(df$TermDate)))


					#Set some targets
					df$term1 <- ifelse(df$TermDate - max(as.Date(df$cycledate)) < 365,1,0)
					df$term0.5 <- ifelse(df$TermDate - max(as.Date(df$cycledate)) < 180,1,0)
					df$term0.25 <- ifelse(df$TermDate - max(as.Date(df$cycledate)) < 90,1,0)
					df$keep <- ifelse(df$Value %like% '%@%',0,1)

					#Omit variables that aren't present for at least x% of the population
					howManyObsPerValue <- aggregate(UserID ~ Value,df,function(x){length(unique(x))})
					keepValues <- howManyObsPerValue[howManyObsPerValue$UserID >= length(unique(df$UserID)) * 0.05,]
					df <- df[df$keep == 1 & df$Value %in% keepValues$Value,]
					dfTable <- as.data.table(df)

					if(nrow(df) > 0){
						widedfTable <- dcast(dfTable,term1 + term0.5 + term0.25 + UserID ~ Value,length)
						widedf <- as.data.frame(widedfTable)


						colnames(widedf) <- paste('v',colnames(widedf),sep='')
						colnames(widedf) <- gsub(pattern=' ',replacement='',colnames(widedf))
						colnames(widedf) <- gsub(pattern='[[:punct:]]',replacement='',colnames(widedf))
						colnames(widedf) <- gsub(pattern='-',replacement='',colnames(widedf))
						colnames(widedf) <- gsub(pattern='[^[:alnum:]]',replacement='',colnames(widedf))
						colnames(widedf) <- gsub(pattern='/',replacement='',colnames(widedf))
						df2 <- widedf[,c(1,5:ncol(widedf))]

						if(sum(df2$vterm1) > 15){

							#Remove cross-validation, since out-of-bag approximates it
							#Upsample the turnovers, at the expense of gaining some false positives

							trainUp <- df2[df2$vterm1 ==1,]
							numReplicates <- round((nrow(df2)/nrow(trainUp))/2)
							trainUpRep <- do.call(rbind, replicate(numReplicates, trainUp, simplify = FALSE))
							train <- rbind(df2,trainUpRep)

							#Fit the randomForest model
							rf <- randomForest(as.factor(vterm1) ~ .,data=train,ntree = 150)
							preds <- predict(rf,newdata=df2)
							importanceList = data.frame(var = colnames(df2[,2:ncol(df2)]),importance(rf))
							importanceListFinal <- importanceList

							modelResults <- table(prediction = preds,actual=df2$vterm1)

							importanceListFinalUnlist <- importanceListFinal
							avgVarImportance <- aggregate(MeanDecreaseGini ~ var,data=importanceListFinalUnlist,mean)
							avgVarImportanceOrder <- avgVarImportance[order(-avgVarImportance$MeanDecreaseGini),]

							modelResultsUnlist <- modelResults
							modelResultsFinal <- aggregate(Freq ~ prediction + actual,data=modelResultsUnlist,sum)
							modelResultsFinal$AvgFreq <- modelResultsFinal$Freq

							save(rf,file=paste('C:/Users/BobKill/OneDrive - Quantum Workplace/Turnover/Calculate turnover/ClientModels/',uniqueOrgNums$OrganizationID[i],'_',uniqueOrgSurveyID,'mod','.RData',sep=''))
						}#End of if(sum(df2$vterm1) > 29)
					}#End of if(nrow(df) > 0)
				}#End of if(nrow(df) > 99)
			}#End of if(is.data.frame('df'))
			if(exists('avgVarImportanceOrder')){
				completeStorTemp <- list(uniqueOrgNums$OrganizationID[i],uniqueOrgSurveyID,avgVarImportanceOrder[1:20,],modelResultsFinal)
				completeStor <- c(completeStor,completeStorTemp)
				save(completeStor,file='C:/Users/BobKill/OneDrive - Quantum Workplace/Turnover/Calculate turnover/completeStorV9.RData')
			}#End of if(exists('avgVarImportanceOrder'))
			gc()
			rm(list=setdiff(ls(), c('i','dbhandle','uniqueOrgNums','completeStor','library.data.table','library.DescTools','library.Matrix','library.RODBC','library.reshape2','library.randomForest')))
			}#end of if(nrow(dfSurvey) > 0){
	}#End of if(exists('dfSurvey')){
}#End of for(i in 1:length(uniqueOrgNums$OrganizationID))




}


