
# Main function to handle processing for different DataVault entity types.
def rvProcessing(self, eRawDF, eimDF, varDataDomain, varSecurityClassification, varPartitionList, varExtractType, fileNum):
    # Step 1: Check for datavault entity types
    entityTypeList = self.getEntityTypeList(eimDF)
    print(entityTypeList)

    # Step 2: Process HUB tables
    if 'hub' in entityTypeList:
        self.processHubTables(eimDF, varPartitionList, fileNum)

# Extracts and returns a list of entity types from the dataframe.
def getEntityTypeList(self, eimDF):
    return eimDF.select(F.col('dvTableType')).distinct().rdd.map(lambda x: x.dvTableType).collect()

# Extracts a list of hub tables and their source versions.
def getHubList(self, eimDF):
    return eimDF.where(F.lower(eimDF.dvTableType) == 'hub').select(F.col('dvTable'), F.col('sourceVersion')).distinct().rdd.map(lambda x: (x.dvTable, x.sourceVersion)).collect()

# Generates schema for a specific hub.
def generateHubSchema(self, eimDF, hub, sourceVersion):
    eimDF = eimDF.where((eimDF.dvTable == hub) & (eimDF.sourceVersion == sourceVersion)).orderBy(F.col('dvColumnOrder'))
    return self.generateSchema(eimDF)

# Returns the ordered columns (hash and non-hash) for the hub table.
def getHubColumns(self, eimDF, hub, sourceVersion):
    eimDF = eimDF.where((eimDF.dvTable == hub) & (eimDF.sourceVersion == sourceVersion)).orderBy(F.col('dvColumnOrder'))

    # Get column order
    hashColumns = eimDF.where(eimDF.dvColumnType == 'hash').orderBy(F.col('dvColumnOrder')).select(F.col('dvColumn')).rdd.map(lambda x: x.dvColumn).collect()
    nonHashColumns = eimDF.where(eimDF.dvColumnType != 'hash').orderBy(F.col('dvColumnOrder')).select(F.col('dvColumn')).rdd.map(lambda x: x.dvColumn).collect()
    businessKeyList = eimDF.where(F.lower(eimDF.dvColumnType) == 'businesskey').orderBy(F.col('dvColumnOrder')).select(F.col('dvColumn')).rdd.map(lambda x: x.dvColumn).collect()

    hubMetadataColumns = ['h_record_source', 'h_load_date_time']
    loadMetadataColumns = ['md_created_dt']
    finalHubColumnOrder = hashColumns + nonHashColumns + hubMetadataColumns + loadMetadataColumns

    # Dataframe selection and hash PK for hubs
    hubSelectList = eimDF.orderBy(F.col('dvColumnType')).select(F.col('dvColumn'), F.col('sourceColumn')).rdd.map(lambda x: (x.dvColumn, x.sourceColumn)).collect()
    hubSelectList = [[x[0], x[1].split('.')[1] if len(x[1].split('.')) == 2 else F.concat_ws('||', x[1])] for x in hubSelectList]

    return finalHubColumnOrder, hubSelectList

# Applies hash logic and prepares final dataframe for the hub.
def hashHubData(self, hubDF, finalHubColumnOrder, hubSelectList):
    hubDF = hubDF.hashWithPK(finalHubColumnValues)
    
    for h, x, y in hubSelectList:
        if h == 'hash':
            if len(y) > 1:
                hubDF = hubDF.withColumn(x, F.sha1(F.col(x)))
            else:
                hubDF = hubDF.withColumn(x, F.when(F.col(f"{x}").cast("string") == '-99', '-99').otherwise(F.sha1(F.concat_ws('||', x))))
    
    return hubDF.distinct().select(*finalHubColumnOrder)

# Identifies and returns the partition columns for the hub.
def getPartitionColumns(self, hubDF, varPartitionList):
    if len(varPartitionList) > 0:
        hubPartitionList = [x for x in varPartitionList if x in hubDF.columns]
        if not hubPartitionList:
            hubPartitionList = ['md_created_dt']
    else:
        hubPartitionList = ['md_created_dt']
    
    return hubPartitionList

# Generates the storage path for the hub table.
def generateHubPath(self, eimDF, hub):
    hubDomainList = eimDF.select('dataDomain').distinct().rdd.map(lambda x: x.dataDomain).collect()
    hubClassificationList = eimDF.select('securityClassification').distinct().rdd.map(lambda x: x.securityClassification).collect()
    
    if len(hubDomainList) != 1 or len(hubClassificationList) != 1:
        raise Exception(f"Hub table has more than one domain or classification")

    varHubPatch = "/{rawVault}/hubs/{}".format(hubDomainList[0], hubClassificationList[0], hub)
    return self.eimRawVaultPath + varHubPatch

# Checks if the Delta file path exists.
def checkDeltaFileExists(self, hubPath):
    return genericModules.utils.deltafileCheck(hubPath)

# Aligns the schema of the existing Delta table with the new data.
def alignSchema(self, deltaHubDF, hubDF):
    rvSchema = deltaHubDF.schema
    for field in rvSchema.fields:
        fieldName = field.name
        fieldDataType = field.dataType
        hubDF = hubDF.withColumn(fieldName, hubDF[fieldName].cast(fieldDataType))

    return hubDF


# Removes duplicates from the existing data in the Delta table.
def removeDuplicates(self, deltaHubDF, hubDF): 
    return hubDF.alias('a').join(deltaHubDF.alias('b'), on=hashColumns, how='LEFT_ANTI').select('a.*').distinct()

# Appends new data to the Delta table.
def appendToDeltaTable(self, deltaHubDF, hubPartitionList, hubPath, fileNum, hub):
    try:
        genericModules.write.appendDeltaTable(deltaHubDF, hubPartitionList, hubPath, fileNum)
        print(f"Appending data to {hub} successful")
    except Exception as e:
        print(f"Failed appending data to {hub}: {e}")
        raise

# Overwrites the existing Delta table with new data.
def overwriteDeltaTable(self, hubDF, hubPartitionList, hubPath, fileNum, hub):
    try:
        genericModules.write.overwriteDeltaTable(hubDF, hubPartitionList, hubPath, fileNum)
        print(f"Overwriting data to {hub} successful")
    except Exception as e:
        print(f"Failed overwriting data to {hub}: {e}")
        raise

# Writes the hub dataframe to Delta Lake, either appending or overwriting data.
def writeToDelta(self, hubDF, hubPartitionList, eimDF, hub, fileNum):
    # Step 1: Generate the storage path
    hubPath = self.generateHubPath(eimDF, hub)
    print(hubPath)

    # Step 2: Check if the path exists and process data
    if pathExists := self.checkDeltaFileExists(hubPath):
        deltaHubDF = DeltaTable.forPath(spark, hubPath).toDF()
        hubDF = self.alignSchema(deltaHubDF, hubDF)

        # Step 3: Append or overwrite the data
        deltaHubDF = self.removeDuplicates(deltaHubDF, hubDF)

        if deltaHubDF.count() > 0:
            self.appendToDeltaTable(deltaHubDF, hubPartitionList, hubPath, fileNum, hub)
        else:
            self.overwriteDeltaTable(hubDF, hubPartitionList, hubPath, fileNum, hub)
    else:
        self.overwriteDeltaTable(hubDF, hubPartitionList, hubPath, fileNum, hub)

#  Processes all hub tables based on the extracted list of hubs.
def processHubTables(self, eimDF, varPartitionList, fileNum):
    hubList = self.getHubList(eimDF)
    print(hubList)

    for hub, sourceVersion in hubList:
        # Step 3: Generate schema for the hub
        hubSchema = self.generateHubSchema(eimDF, hub, sourceVersion)
        print(hubSchema)

        # Step 4: Process hub columns
        finalHubColumnOrder, hubSelectList = self.getHubColumns(eimDF, hub, sourceVersion)
        print(finalHubColumnOrder)

        # Step 5: Apply hashing and prepare final dataframe
        hubDF = self.hashHubData(eimDF, finalHubColumnOrder, hubSelectList)

        # Step 6: Partitioning
        hubPartitionList = self.getPartitionColumns(hubDF, varPartitionList)
        print(f"hub partition list {hubPartitionList}")

        # Step 7: Process hubs into Delta files
        self.writeToDelta(hubDF, hubPartitionList, eimDF, hub, fileNum)
