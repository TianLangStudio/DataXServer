*DataX Server*
===============     
   生产环境用的基于阿里早期开源的DataX1.0.0 
   当前版本基于新开源的DataX 3.0.0 重构 还未用在生产环境
   为 [DataX](https://github.com/alibaba/DataX) 提供远程调用（Thrift Server， Http Server）分布式运行（DataX On YARN）功能
   
**Feature**
---------------
1. Thrift Server 
2. DataX on Yarn
3. Http Server （待开发）

**Develop**
---------------  
  项目依赖阿里 DataX  
  git https://github.com/alibaba/DataX.git 
  cd DataX    
  mvn install
  
  git https://github.com/TianLangStudio/DataXServer.git  
  cd DataXServer  
  mvn compile  
  
