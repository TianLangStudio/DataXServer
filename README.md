*DataX Server*
================  

   生产环境用的基于阿里早期开源的DataX1.0.0        
   当前版本基于新开源的DataX 3.0.0 重构 还未用在生产环境   
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
- ~~1.Http Server(已完成)~~   
- ~~2.代码重构 (已完成)~~    
- ~~3.按照功能类型拆分到多个子项目中　重新组织包名　方便后续新增功能~~
- 4.完善文档示例

## Deploy
   下载发布包[DataXServer-0.0.1.tar.gz](http://pan.baidu.com/s/1hrHcbqs) 并解压 进入 0.0.1 目录     
   
   执行   ./bin/startThriftServer.sh  启动Thrift Server     
   
   进入 example/nodejs 目录      
   
   执行 node submitStream2Stream.js  提交测试任务到Thrift Server     
   
   
   
**Develop**
---------------  
  项目依赖阿里 DataX  
  git clone https://github.com/alibaba/DataX.git 
  cd DataX    
  mvn install
  
  git clone https://github.com/TianLangStudio/DataXServer.git  
  cd DataXServer  
  mvn compile  
## 完善文档
TODO
## 问题交流可加群
QQ群：579896894
----------------
![KeepLearning QQ](https://raw.githubusercontent.com/TianLangStudio/DataXServer/master/images/tianlangstudio-keeplearning-qrcode.jpg)  
