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
dbhandle <- odbcDriverConnect('driver={SQL Server};server=104.130.165.170;database=Platform;uid=sa;pwd=Quantum!00')
uniqueOrgNums <- sqlQuery(dbhandle, 
	'select distinct
		OrganizationID 
	from platform.dbo.DataMLRK2'
)


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
			uniqueOrgSurveyID <- uniqueOrgSurveyID[length(uniqueOrgSurveyID)-1]

			df <- dfSurvey[dfSurvey$surveyRankID <= uniqueOrgSurveyID,]

			if(is.data.frame(df)){

				if(nrow(df) > 99){

					#Do some basic date formatting
					df$HireDate <- as.Date(df$HireDate)
					df$TermDate <- as.Date(ifelse(is.na(df$TermDate),'9999-12-31',as.character(df$TermDate)))


					#Set some targets
					df$term1 <- ifelse(df$TermDate - max(as.Date(df$cycledate)) < 365,1,0)
					df$term0.5 <- ifelse(df$TermDate - max(as.Date(df$cycledate)) < 180,1,0)
					df$term0.25 <- ifelse(df$TermDate - max(as.Date(df$cycledate)) < 90,1,0)
					df$keep <- ifelse(df$Value %like% '%@%',0,1)

					#Omit variables that aren't present for at least 5% of the population
					howManyObsPerValue <- aggregate(UserID ~ Value,df,function(x){length(unique(x))})
					keepValues <- howManyObsPerValue[howManyObsPerValue$UserID >= length(unique(df$UserID))*0.05,]
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
							rf <- randomForest(as.factor(vterm1) ~ .,data=train,ntree=150)
							preds <- predict(rf,newdata=df2)
							importanceList = data.frame(var = colnames(df2[,2:ncol(df2)]),importance(rf))
							importanceListFinal <- importanceList

							modelResults <- table(prediction = preds,actual=df2$vterm1)

							importanceListFinalUnlist <- importanceListFinal
							avgVarImportance <- aggregate(MeanDecreaseGini ~ var,data=importanceListFinalUnlist,mean)
							avgVarImportanceOrder <- avgVarImportance[order(-avgVarImportance$MeanDecreaseGini),]

							modelResultsUnlist <- modelResults
							modelResultsFinal <- aggregate(Freq ~ prediction + actual,data=modelResultsUnlist,sum)
							modelResultsFinal$AvgFreq <- modelResultsFinal$Freq/iters

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


