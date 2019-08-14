import Foundation

class DBOperations: NSObject {
    
    fileprivate var objRefDb : DatabaseHelper!
    
    fileprivate let TBL_TASK_SCHEDULE = "task_schedule"
    
    override init() {
        objRefDb = DatabaseHelper()
    }
    
    static let shared = DBOperations()
    
    static func isUserSetWorkingDays() -> Bool {
        var bResult = false
        
        if (Util.userDefaults.value(forKey: Constant.kDEFAULT_WORKING_DAY_DETAILS_KEY) != nil) {
            bResult = Util.userDefaults.value(forKey: Constant.kDEFAULT_WORKING_DAY_DETAILS_KEY) as! Bool
        }
        
        return bResult
        
    }
    
    func getlastRecordID() -> Int {
        
        var iLastrecordId = -1
        objRefDb.openDatabase()
        
        let arrDictData = objRefDb.executeSelectQueryForTableDictionary(strSelectQuery: "SELECT id FROM \(TBL_TASK_SCHEDULE) ORDER BY id DESC LIMIT 1")
        
        if arrDictData.count > 0 {
            if let iData = arrDictData[0]["id"] as? Int {
             iLastrecordId = iData
            }
        }
        
        objRefDb.closeDatabase()
        
        return iLastrecordId
    }
    
    @discardableResult func startOperationPerform() -> Int {
        
        objRefDb.openDatabase()

        var jobScheduleDict = [String:Any]()
        jobScheduleDict["vTaskDate"] = Util.getLocalStanderdDateFormat()
        jobScheduleDict["vTaskStartTime"] = Util.getLcoalTimeStampMiliSecond()

        let iLastRecordID = objRefDb.insertOrReplaceIntoTable(tableName: TBL_TASK_SCHEDULE, tableDictData: jobScheduleDict)
        print("DummyTable > Table Insertion Status: \(iLastRecordID)")
        
        objRefDb.closeDatabase()
        return iLastRecordID
    }
    
    func deleteWorkEntery(_ iWorkId : Int) {
        objRefDb.openDatabase()
        
        if objRefDb.deleteRecord(tblName: TBL_TASK_SCHEDULE, columnName: "id", conditionalValue: "\(iWorkId)") {
            print("Work entry deleted")
        }
        else {
            print("Unable to deleted record")
        }
        
        objRefDb.closeDatabase()
    }
    
    @discardableResult func getAllJobTask() -> [[String:Any]] {
        
        objRefDb.openDatabase()
        
        var jobScheduleDict = [[String:Any]]()
        
        //jobScheduleDict = objRefDb.executeSelectQueryForTableDictionary(strSelectQuery: "SELECT SUM(vTaskEndTime - vTaskStartTime) as iTotalWorkTime, SUM(vTaskStartTime) as vTaskStartTime, SUM(vTaskEndTime) as vTaskEndTime , id, vTaskDate FROM \(TBL_TASK_SCHEDULE) WHERE vTaskEndTime != '' GROUP BY vTaskDate ORDER BY vTaskDate DESC")
        
        jobScheduleDict = objRefDb.executeSelectQueryForTableDictionary(strSelectQuery: "SELECT id, vTaskDate, SUM(vTaskEndTime - vTaskStartTime) as iTotalWorkTime, SUM(vTaskStartTime) as vTaskStartTime, SUM(vTaskEndTime) as vTaskEndTime, (SELECT SUM(vTaskEndTime - vTaskStartTime) as iNetTotalWorkTime FROM \(TBL_TASK_SCHEDULE) WHERE vTaskEndTime != '' ) as iNetWorkHours FROM \(TBL_TASK_SCHEDULE) WHERE vTaskEndTime != '' GROUP BY vTaskDate ORDER BY vTaskDate DESC")
        
        
        if Util.userDefaults.value(forKey: Constant.kDEFAULT_WORKING_HOURS) != nil {
            let iTotalWorkingHoursADay = Util.userDefaults.value(forKey: Constant.kDEFAULT_WORKING_HOURS) as! Int
            let iTotalWorkingSec = iTotalWorkingHoursADay*60*60
            
            ///*
            if jobScheduleDict.count > 0 {
                let jobDictData = jobScheduleDict[0]
                let strTime = Util.getWorkingTimeFromMilisecods(jobDictData["iNetWorkHours"] as! Int)
                let arrTimeData = strTime.split(separator: ":")
                
                let iHourTime = Int(arrTimeData[0])
                
                var iMinuteTime = Int(arrTimeData[1])
                if iHourTime! > 0 {
                    iMinuteTime = (iHourTime!*60)+iMinuteTime!
                }
                
                var iSecTime = Int(arrTimeData[2])
                if iMinuteTime! > 0 {
                    iSecTime = (iMinuteTime!*60)+iSecTime!
                }
                
                let percentage = Int(round(Double(iSecTime!*100)/Double(iTotalWorkingSec)))
                var dictData = [String:Any]()
                dictData["pieChart"] = percentage
                jobScheduleDict.insert(dictData, at: 0)
            }
            //*/
            
            /*
             
             let iTotalMiliSeconds = iTotalWorkingSec*1000
             
             var iTotalPercent = 0
             
             let iBusinessDaysHoursMiliSec = Util.getMiliSecondsOfWokringDaysInCurrentMonth()
             
            for jobDictData in jobScheduleDict {
                /*
                let iSingleDayTime = round(Double(jobDictData["iTotalWorkTime"] as! Int)/1000)
                let iNetWorkingTime = round(Double(jobDictData["iNetWorkHours"] as! Int))
                
                //TEST:::
                let percentage = Int(round(Double(iSingleDayTime*100)/Double(iBusinessDaysHoursMiliSec)))
                print("Total % Time: \(percentage)")
                iTotalPercent = iTotalPercent + percentage*/
                
                //TEST:::
                
                let strTime = Util.getWorkingTimeFromMilisecods(jobDictData["iTotalWorkTime"] as! Int)
                let arrTimeData = strTime.split(separator: ":")
                
                let iHourTime = Int(arrTimeData[0])
                
                var iMinuteTime = Int(arrTimeData[1])
                if iHourTime! > 0 {
                    iMinuteTime = (iHourTime!*60)+iMinuteTime!
                }
                
                var iSecTime = Int(arrTimeData[2])
                if iMinuteTime! > 0 {
                    iSecTime = (iMinuteTime!*60)+iSecTime!
                }
               
//                let iMiliSec = iSecTime!*1000
                //let percentage = round(Double(iMiliSec)/Double(iBusinessDaysHoursMiliSec*360))
                //let percentage = Int(round(Double(iMiliSec)/Double(iTotalMiliSeconds)*360))
                let percentage = round(Double(iBusinessDaysHoursMiliSec/1000)/Double(iSecTime!*100))
                print("Total % Time: \(percentage)")
                iTotalPercent = iTotalPercent + Int(percentage)
                
            }*/
            
        }
        
        objRefDb.closeDatabase()
        return jobScheduleDict
    }
    
    @discardableResult func getAllSingleDayJobTask(_ whereCaluesDate: String) -> [[String:Any]] {
        
        objRefDb.openDatabase()
        
        var jobScheduleDict = [[String:Any]]()
        
        jobScheduleDict = objRefDb.executeSelectQueryForTableDictionary(strSelectQuery: "SELECT * FROM \(TBL_TASK_SCHEDULE) WHERE vTaskDate = '\(whereCaluesDate)' AND vTaskEndTime != '' ORDER BY id DESC")
        
        objRefDb.closeDatabase()
        return jobScheduleDict
    }
    
    @discardableResult func pauseOrStopOperationPerform() -> Bool {
        objRefDb.openDatabase()
        
        var jobScheduleDict = [String:Any]()
        jobScheduleDict["vTaskDate"] = Util.getLocalStanderdDateFormat()
        jobScheduleDict["vTaskEndTime"] = Util.getLcoalTimeStampMiliSecond()
        
        var iLastRecordId = getlastRecordID()
        
        if iLastRecordId < 0 {
            iLastRecordId = 0
        }
        let bRecordStatus = objRefDb.updateIntoTable(tableName: TBL_TASK_SCHEDULE, tableDictData: jobScheduleDict, whereClause: "id = \(iLastRecordId)")
        
        objRefDb.closeDatabase()
        return bRecordStatus
    }
    
    
}
