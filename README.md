*DataX Server*
================  

为 [DataX](https://github.com/alibaba/DataX) 提供远程调用（Thrift Server， Http Server）分布式运行（DataX On YARN）功能
   
**Feature**
---------------
- 1. Thrift Server 
- 2. DataX on Yarn
- 3. Http Server 
- 4. 单机多线程方式运行
- 5. 单机多进程方式运行
- 6. 分布式运行(On Yarn)
- 7. 混合模式运行（Yarn+多进程模式运行）
## TODO
- ~~1.Http Server~~   
- ~~2.代码重构~~    
- ~~3.按照功能类型拆分到多个子项目中　重新组织包名　方便后续新增功能~~
- 4.完善文档示例

## Deploy
   下载发布包[DataXServer-0.0.1.tar.gz](http://pan.baidu.com/s/1hrHcbqs) 并解压 进入 0.0.1 目录     
   
   启动Thrift Server
   ```shell
   ./bin/startThriftServer.sh     
   ```
   使用NodeJS提交测试任务到Thrift Server  
   ```shell
   cd example/nodejs    
   node submitStream2Stream.js 
   ```
     
   
   
   
**Develop**
---------------  
  ### 下载程序源码
  __项目依赖阿里 DataX__
  ```bash
  git clone https://github.com/alibaba/DataX.git 
  cd DataX    
  mvn install
  
  git clone https://github.com/TianLangStudio/DataXServer.git  
  cd DataXServer  
  mvn clean compile install -DskipTests
  ```
  ### run http server (已部署好datax 且能正常运行job/test_job.json)
  - 配置datax安装目录
  > 修改pom.xml中的datax-home配置项为部署datax的地址
  ```xml
   <datax-home>/data/test/datax</datax-home>
  ```
  - 启动http server
  ```bash
   cd httpserver
   mvn scala:run -Dlauncher=httpserver -DskipTests
  ```
  - 提交任务 获取任务ID
  ```bash
  curl -XPOST -d "@测试文件路径" 127.0.0.1:9808/dataxserver/task
```
  > tianlang@tianlang:job$ curl  -XPOST -d "@job/test_job.json" 127.0.0.1:9808/dataxserver/task
  > 0 （任务ID）
  - 获取任务执行状态结果耗时
  ```bash
  curl  127.0.0.1:9808/dataxserver/task/status/0
  curl  127.0.0.1:9808/dataxserver/task/0
  curl  127.0.0.1:9808/dataxserver/task/cost/0
```
![运行成功日志](https://raw.githubusercontent.com/TianLangStudio/DataXServer/master/images/test_job_success.png) 
## Document
TODO
## 问题交流可加群
QQ群：579896894
----------------
![KeepLearning QQ](https://raw.githubusercontent.com/TianLangStudio/DataXServer/master/images/tianlangstudio-keeplearning-qrcode.jpg)  
