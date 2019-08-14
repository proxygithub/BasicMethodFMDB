import Foundation
import FMDB

class DatabaseHelper : NSObject, FileManagerDelegate {
    
    var fmDatabase : FMDatabase!
    
    override init() {
        super.init()
        fmDatabase = FMDatabase(path: DatabaseHelper.getFilePath(filename: "TestPractical.sqlite"))
    }
    
    func openDatabase() -> Void {
        if !fmDatabase.open() {
            print("Databse open error")
        }
    }
    
    func closeDatabase() -> Void {
        if fmDatabase.open() {
            fmDatabase.close()
        }
    }
    
    static func getFilePath(filename: String) -> String {
        let paths = NSSearchPathForDirectoriesInDomains(.documentDirectory, .userDomainMask, true)
        let docDirPath = paths[0]
        return docDirPath.appending("/\(filename)")
    }
    
    //MARK: create database from Assets
    static func copyDatabaseFromAssests(databaseName:String) -> Bool {
        
        var isCreate: Bool = false
        let fileManager = FileManager()
        let databasePath: String = DatabaseHelper.getFilePath(filename: databaseName)
        
        print("Databasepath: \(databasePath)")
        if !fileManager.fileExists(atPath: databasePath) {
            let seprateData = databaseName.components(separatedBy: ".sqlite")[0]//remove .sqlite from string
            let fromPath = Bundle.main.path(forResource: seprateData, ofType: "sqlite")
            isCreate = ((try? fileManager.copyItem(atPath: fromPath!, toPath: databasePath)) != nil)
        } else {
            isCreate = true
        }
        return isCreate
    }
    
    //MARK: Delete Database
    static func deleteDatabaseFromDevice(databaseName: String) -> Bool {
        
        var isCreate:Bool = false
        
        let fileManager = FileManager()
        let DBFilePath = DatabaseHelper.getFilePath(filename: databaseName)
        
        if fileManager.fileExists(atPath: DBFilePath) {
            isCreate = ((try? fileManager.removeItem(atPath: DBFilePath)) != nil)
        } else {
            isCreate = true
        }
        return isCreate
    }
    
    //MARK: Retrive Table Schema
    func getTableSchemas(tableName:String) -> [[String:Any]] {
        
        var arrTableSchema = [[String:Any]]()
        
        fmDatabase.open()
        let resultSet:FMResultSet = fmDatabase.getTableSchema(tableName)!
        
        while resultSet.next() {
            arrTableSchema.append((resultSet.resultDictionary as? [String:Any])!)
        }
        
        fmDatabase.close()
        
        return arrTableSchema
    }
    
    //MARK: Insertion Operations
    func insertIntoTable(tableName:String, tableDictData: [String:Any]) -> Bool {
        
        fmDatabase.open()
        
        let strDictData = self.dictionaryToValues(dictData: tableDictData)
        
        let query = "INSERT INTO "+tableName+" "+strDictData
        let result : Bool = fmDatabase.executeUpdate(query, withArgumentsIn: [])
        
        fmDatabase.close()
        
        return result
    }
    
    func insertOrReplaceIntoTable(tableName:String, tableDictData: [String:Any]) -> Int {
        
        fmDatabase.open()
        var rowInserted:Int = -1
        
        let strDictData = self.dictionaryToValues(dictData: tableDictData)
        
        let query = "INSERT OR REPLACE INTO "+tableName+" "+strDictData
        let result : Bool = fmDatabase.executeUpdate(query, withArgumentsIn: [])        
        
        if !result {
            print(fmDatabase.lastError())
        }
        else {
            rowInserted = Int(fmDatabase.lastInsertRowId)
        }
        fmDatabase.close()
        return rowInserted
    }
    
    func insertOrUpdateIntoTable(tableName:String, tableDictData: [String:Any]) -> Int {
        
        fmDatabase.open()
        var rowInserted:Int = -1
        
        let strDictData = self.dictionaryToValues(dictData: tableDictData)
        
        let query = "INSERT OR UPDATE INTO "+tableName+" "+strDictData
        let result : Bool = fmDatabase.executeUpdate(query, withArgumentsIn: [])
        
        if !result {
            print(fmDatabase.lastError())
        }
        else {
            rowInserted = Int(fmDatabase.lastInsertRowId)
        }
        
        fmDatabase.close()
        
        return rowInserted
    }
    
    func insertOrIgnoreIntoTable(tableName:String, tableDictData: [String:Any]) -> Int {
        
        fmDatabase.open()
        var rowInserted:Int = -1
        
        let strDictData = self.dictionaryToValues(dictData: tableDictData)
        
        let query = "INSERT OR IGNORE INTO "+tableName+" "+strDictData
        let result : Bool = fmDatabase.executeUpdate(query, withArgumentsIn: [])
        
        if !result {
            print(fmDatabase.lastError())
        }
        else {
            rowInserted = Int(fmDatabase.lastInsertRowId)
        }
        
        fmDatabase.close()
        
        return rowInserted
    }
    
    func bulkInsertOrReplaceIntoTable(tableName:String, Insertvalues: [[String:Any]]) -> Bool {
        
        var result : Bool = false
        var arrColumnNames = getColumnNames(forTableName: tableName)
        
        fmDatabase.open()
        
        var appendColumnData : String = String()
        var arrInsertionColumnName = [String]()
        
        for iCounter in 0..<Insertvalues.count {
            let arrDictData : [String:Any] = Insertvalues[iCounter]
            appendColumnData.append(" (")
            
            for columnIndex in 0..<arrColumnNames.count {
                let strColName : String = arrColumnNames[columnIndex]
                var strColumnData : String
                
                if arrDictData[strColName] != nil {
                    
                    if iCounter == 0 {
                        arrInsertionColumnName.append(strColName)
                    }
                    
                    if let strColumnDataIF : String = String(describing: arrDictData[strColName]!) {
                        strColumnData = strColumnDataIF as String
                        strColumnData = strColumnData.replacingOccurrences(of: "'", with: "''")
                        appendColumnData.append(" '"+strColumnData+"',")
                    }
                }
            }
            
            let iStringCount = appendColumnData.count
            var removeLastComa:String = (appendColumnData as NSString).substring(to: iStringCount - 1)
            appendColumnData = ""
            appendColumnData = removeLastComa
            
            removeLastComa = ""
            appendColumnData = appendColumnData.appending("),")
            
        }
        
        let columns : String = "('\(arrInsertionColumnName.joined(separator: "','"))')"
        var insertQuery : String = "INSERT OR REPLACE INTO "+tableName+" "+columns+" VALUES"
        
        var removeComa:String = (appendColumnData as NSString).substring(to: appendColumnData.count - 1)
        appendColumnData = ""
        appendColumnData = removeComa
        
        removeComa = ""
        insertQuery = insertQuery.appending(" "+appendColumnData+"")
        result = executeUpdateQuery(updateQuery: insertQuery)
        
        fmDatabase.close()
        return result
    }
    
    func bulkInsertIntoTable(tableName:String, Insertvalues:[[String:Any]]) -> Bool {
        
        var result : Bool = false
        var arrColumnNames = getColumnNames(forTableName: tableName)
        
        fmDatabase.open()
        
        var appendColumnData : String = String()
        var arrInsertionColumnName = [String]()
        
        for iCounter in 0..<Insertvalues.count {
            let arrDictData : [String:Any] = Insertvalues[iCounter]
            appendColumnData.append(" (")
            
            for columnIndex in 0..<arrColumnNames.count {
                let strColName : String = arrColumnNames[columnIndex]
                var strColumnData : String
                
                if arrDictData[strColName] != nil {
                    
                    if iCounter == 0 {
                        arrInsertionColumnName.append(strColName)
                    }
                    
                    if let strColumnDataIF : String = String(describing: arrDictData[strColName]!) {
                        strColumnData = strColumnDataIF as String
                        strColumnData = strColumnData.replacingOccurrences(of: "'", with: "''")
                        appendColumnData.append(" '"+strColumnData+"',")
                    }
                }
            }
            
            let iStringCount = appendColumnData.count
            var removeLastComa:String = (appendColumnData as NSString).substring(to: iStringCount - 1)
            appendColumnData = ""
            appendColumnData = removeLastComa
            
            removeLastComa = ""
            appendColumnData = appendColumnData.appending("),")
            
        }
        
        let columns : String = "('\(arrInsertionColumnName.joined(separator: "','"))')"
        var insertQuery : String = "INSERT INTO "+tableName+" "+columns+" VALUES"
        
        var removeComa:String = (appendColumnData as NSString).substring(to: appendColumnData.count - 1)
        appendColumnData = ""
        appendColumnData = removeComa
        
        removeComa = ""
        insertQuery = insertQuery.appending(" "+appendColumnData+"")
        result = executeUpdateQuery(updateQuery: insertQuery)
        
        fmDatabase.close()
        
        return result
    }
    
    //MARK: Select Operations
    func isTableDataAvailable(tableName : String) -> Bool {
        
        fmDatabase.open()
        
        let resultSet : FMResultSet = fmDatabase.executeQuery("SELECT COUNT(*) FROM \(tableName)", withArgumentsIn: [])!
        
        if resultSet.next() {
            let count = resultSet.int(forColumnIndex: 0)
            
            if count > 0 {
                return false
            }
            else {
                return true
            }
        }
        else {
            print("Database Error")
        }
        
        fmDatabase.close()
        
        return false
    }
    
    func getNumOfTableDataCount(tableName: String) -> Int32 {
        
        fmDatabase.open()
        
        let resultSet : FMResultSet = fmDatabase.executeQuery("SELECT COUNT(*) FROM \(tableName)", withArgumentsIn: [])!
        
        if resultSet.next() {
            
            let count = resultSet.int(forColumnIndex: 0)
            
            if count > 0 {
                return count
            }
            else {
                return 0
            }
        }
        else {
            print("Database Error")
        }
        
        fmDatabase.close()
        
        return 0
    }
    
    fileprivate func getColumnNames(forTableName: String) -> [String] {
        let arrColmnNames : [ [String:Any] ] = getTableSchemas(tableName: forTableName)
        var arrColumnsName = [String]()
        
        for columnsData in arrColmnNames {
            if let strColumNm = columnsData["name"] as? String {
                arrColumnsName.append(strColumNm)
            }
        }
        return arrColumnsName
    }
    
    func executeSelectAllQueryForSingleTable(strQueryTablename: String) -> [[String:Any]] {
        
        var arrData = [[String:Any]]()
        
        fmDatabase.open()
        
        do {
            let resultSet : FMResultSet = try fmDatabase.executeQuery("SELECT * FROM \(strQueryTablename)", values: nil)
            
            while resultSet.next() {
                arrData.append(validateDictionary(resultSet.resultDictionary as! [String : Any]))
            }
        }
        catch let exceptionError {
            print("Sqlite :: \(exceptionError.localizedDescription)")
        }
        
        fmDatabase.close()
        
        return arrData
    }
    
    func validateDictionary(_ dictData : [String : Any]) -> [String : Any] {
        
        let arrAllKeys : [String] = [String](dictData.keys)
        var validateDict = [String : Any]()
        
        for dictDataItem in arrAllKeys {
            //Remove columns which has Null values
            if !(dictData[dictDataItem]! is NSNull) {
                validateDict[dictDataItem] = dictData[dictDataItem]!
            }
        }
        
        return validateDict
    }
    
    func validateDictionaryColumnSpecific(_ dictData : [String : Any], selectedColumn: String) -> String {
        
        var validateDict = ""
        //Remove columns which has Null values
        if !(dictData[selectedColumn]! is NSNull) {
            validateDict = "\(dictData[selectedColumn]!)"
        }
        return validateDict
    }
    
    
    func executeSelectQueryForTableDictionary(strSelectQuery : String) -> [[String:Any]] {
        
        fmDatabase.open()
        
        var arrData = [[String:Any]]()
        
        do {
            let resultSet : FMResultSet = try fmDatabase.executeQuery("\(strSelectQuery)", values: nil)
            while resultSet.next() {
                arrData.append(validateDictionary(resultSet.resultDictionary as! [String : Any]))
            }
        }
        catch let exceptionError {
            print("Sqlite :: \(exceptionError.localizedDescription)")
        }
        
        fmDatabase.close()
        
        return arrData
    }
    
    func executeSelectQueryToFetchSingleColumn(strTableName:String, columnName: String) -> [String] {
        
        var arrData = [String]()
        fmDatabase.open()
        
        do {
            let resultSet : FMResultSet = try fmDatabase.executeQuery("SELECT * FROM \(strTableName)", values: [])
            while resultSet.next() {
                if validateDictionaryColumnSpecific(resultSet.resultDictionary as! [String:Any], selectedColumn: columnName).count > 0 {
                    arrData.append(validateDictionaryColumnSpecific(resultSet.resultDictionary as! [String:Any], selectedColumn: columnName))
                }
            }
        }
        catch let exceptionError {
            print("Sqlite :: \(exceptionError.localizedDescription)")
        }
        
        fmDatabase.close()
        return arrData
    }
    
    func executeSelectQueryWithWhereClause(strQuery:String, whereClause: String) -> [[String:Any]] {
        
        var arrData = [[String:Any]]()
        fmDatabase.open()
        
        do {
            let resultSet : FMResultSet = try fmDatabase.executeQuery("\(strQuery) WHERE \(whereClause)", values: [])
            while resultSet.next() {
                arrData.append(validateDictionary(resultSet.resultDictionary as! [String : Any]))
            }
        }
        catch let exceptionError {
            print("Sqlite :: \(exceptionError.localizedDescription)")
        }
        fmDatabase.close()
        return arrData
    }
    
    //MARK:- Update Operations
    func updateIntoTable(tableName:String, tableDictData: [String:Any], whereClause:String) -> Bool {
        
        fmDatabase.open()
        var rowInserted:Bool = false
        let strDictData = self.dictionaryToUpdateValues(dictData: tableDictData)
        
        let query = "UPDATE "+tableName+" SET "+strDictData+" WHERE "+whereClause
        rowInserted = fmDatabase.executeUpdate(query, withArgumentsIn: [])
        
        if !rowInserted {
            print(fmDatabase.lastError())
        }
        
        fmDatabase.close()
        
        return rowInserted
    }
    
    func executeUpdateQuery(updateQuery:String) -> Bool {
        
        var isUpdate:Bool = false
        
        fmDatabase.open()
        
        isUpdate = fmDatabase.executeUpdate(updateQuery, withArgumentsIn: [])
        
        if !isUpdate {
            print(fmDatabase.lastErrorMessage())
        }
        
        fmDatabase.close()
        
        return isUpdate
    }
    
    //MARK:- Delte Operations
    func deleteRecord(tblName : String , columnName : String , conditionalValue : String) -> Bool {
        let deleted = self.executeUpdateQuery(updateQuery: "DELETE FROM \(tblName) WHERE \(columnName) = '\(conditionalValue)'")
        return deleted
    }
    
    func deleteRecordWithAndQuery(tblName : String , columnName1 : String , conditionalValue1 : String, columnName2 : String, conditionalValue12 : String) -> Bool {
        let deleted = self.executeUpdateQuery(updateQuery: "DELETE FROM \(tblName) WHERE \(columnName1) = '\(conditionalValue1)' AND \(columnName2) = '\(conditionalValue12)'")
        return deleted
    }
    
    func deleteAllRecord(tblName : String) ->Bool {
        let deleted = self.executeUpdateQuery(updateQuery: "DELETE FROM \(tblName)")
        return deleted
    }
    
    
    //MARK:- Dictionary value to String for insertion
    
    fileprivate func dictionaryToValues(dictData: [String:Any]) -> String {
        
        var strValues:String = String()
        
        let arrAllKeys : [String] = [String](dictData.keys)
        var strKeys:String = "("
        var strData:String = "("
        for index in 0..<arrAllKeys.count {
            strKeys = strKeys.appending(arrAllKeys[index]+",")
            let strVal:String = String(describing: dictData[arrAllKeys[index]]!)
            let Val = strVal.replacingOccurrences(of: "'", with: "''")
            strData = strData.appending("'"+Val+"',")
        }
        
        let iKeys = strKeys.count
        let iDatas = strData.count
        
        strKeys = (strKeys as NSString).substring(to: iKeys - 1)
        strData = (strData as NSString).substring(to: iDatas - 1)

        strValues = strKeys+") VALUES "+strData+")"
        
        return  strValues
    }
    
    //MARK:- Dictionary value to String for updation
    fileprivate func dictionaryToUpdateValues(dictData: [String:Any]) -> String {
        
        var strValues:String = String()
        
        let arrAllKeys : [String] = [String](dictData.keys)
        
        for index in 0..<arrAllKeys.count {
            
            let strColName : String = arrAllKeys[index]
            var strColumnData : String
            
            if dictData[strColName] != nil {
                
                if let strColumnDataIF : String = String(describing: dictData[strColName]!) {
                    strColumnData = strColumnDataIF as String
                    strColumnData = strColumnData.replacingOccurrences(of: "'", with: "''")
                    if (strColumnData.count > 0) {
                        strValues = strValues.appending(strColName+" = '"+strColumnData+"',")
                    }
                    else {
                        strValues = strValues.appending(strColName+" = null,")
                    }
                }
            }
            else {
                strValues = strValues.appending(strColName+" = null,")
            }
        }
        
        let iStrValues = strValues.count
        strValues = (strValues as NSString).substring(to: iStrValues - 1)
        
        return  strValues
    }
    
   fileprivate func getValidString(stringData:NSString) -> String {
        var strData : NSString = stringData
        
        if strData.length == 0 || strData == "(null)" || strData == "<null>" {
            strData = ""
        }
        return strData as String
    }
    
    //MARK::::- Funciton to create SqliteFunction in Sqlite and fetch query result
    
    //    func circleSqliteFunction(with centerLat: Double, withLong centerLong: Double, withRadius circleRadius: Double) -> [AnyHashable]? {
    //        var arrCombineData: [AnyHashable] = []
    //
    //        fmDatabase.makeFunctionNamed("radiusData", maximumArguments: 4, with: { context, argc, argv in
    //
    //            let dLat = sqlite3_value_double(OpaquePointer(argv?[0]))
    //            let dLong = sqlite3_value_double(OpaquePointer(argv?[1]))
    //            let dColumnLat = sqlite3_value_double(OpaquePointer(argv?[2]))
    //            let dColumnLong = sqlite3_value_double(OpaquePointer(argv?[3]))
    //
    //            let hSine = Haversine(lat1: Float(dLat), lon1: Float(dLong), lat2: Float(dColumnLat), lon2: Float(dColumnLong))
    //            let result = hSine?.toMeters()
    //
    //            sqlite3_result_double(OpaquePointer(context), Double(result!))
    //
    //        })
    //
    //
    ////      let query = formatString("select * from (SELECT 'circle_site' AS site_or_sr, *, cm.vCity, radiusData(%f,%f,vLatitude,vLongitude) AS Distance FROM site_mas sm LEFT JOIN address_mas am ON sm.iAddressId =am.iAddressId LEFT JOIN zipcode_mas zm ON zm.iZipcode = am.iZipcode  LEFT JOIN city_mas cm ON cm.iCityId = zm.iCityId WHERE sm.iStatus = '1'", centerLat, centerLong, centerLat, centerLong, circleRadius)
    ////
    //
    //        let query = ("select *, radiusData(%f,%f,latitude,longitude) AS distance from content having distance < %f ORDER BY distance" + "\(centerLat)," + "\(centerLong)," + "\(centerLat)," + "\(centerLong)," + "\(circleRadius)")
    //
    //        let objDatabaseHelper = DatabaseHelper()
    //
    //         let arrOfTableColumn = ["content_id","content_area","content_city","contact_person_mob_no","contact_person_name","content_address1","content_address2","content_server_id","created_date","date_time","description","latitude","longitude","content_state","content_status","sync_status","title","updated_date","user_id","content_zip_code"]
    //
    //        objDatabaseHelper.executeSelectQuery(selectQuery: query, tableColumnNames: arrOfTableColumn)
    //
    //        return arrCombineData
    //    }
}

