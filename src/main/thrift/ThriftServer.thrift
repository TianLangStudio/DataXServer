namespace java com.tianlangstudio.data.datax.ext.thrift
struct TaskCost{
    1:string beginTime
    2:string entTime
    3:string cost
}
struct TaskResult{
    1:bool success = true
    2:string msg = "success"
}
service ThriftServer {
     string submitJob(1:string jobConfPath)
     string submitJobWithParams(1:string jobConfPath,2:map<string,string> params)
     string getJobStatus(1:string jobId)
     bool cancelJob(1:string jobId)
     TaskResult getJobResult(1:string jobId)
     TaskCost   getJobCost(1:string jobId)
}