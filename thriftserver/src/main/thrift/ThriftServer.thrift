namespace java org.tianlangstudio.data.hamal.server.thrift
struct ThriftTaskCost{
    1:i64 beginTime
    2:i64 endTime
    3:i64 cost
}
struct ThriftTaskResult{
    1:bool success = true
    2:string msg = "success"
}
service ThriftServer {
     string submitTask(1:string taskConfPath)
     string submitTaskWithParams(1:string taskConfPath,2:map<string,string> params)
     string getTaskStatus(1:string taskId)
     bool cancelTask(1:string taskId)
     ThriftTaskResult getThriftTaskResult(1:string taskId)
     ThriftTaskCost   getThriftTaskCost(1:string taskId)
}