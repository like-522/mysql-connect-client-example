# 学习记录,记录开发的功能以及实现的功能
## 实现功能 ##
## 实现Java定义的 java.sql包下的规范接口.
1. 仅实现4.1新连接协议与mysql进程服务器建立连接.
2. 实现预编译的statement语句,可以发送参数查询.占位符? ServerPreparedStatement
3. 实现普通的statement语句,不是预编译的实现 StatementImpl
4. 读取配置文件的配置,以及自定义一些配置(配置在url中)可以读取. 
5. 客户端预编译功能, 客户端预编译是客户端缓存参数和占位符,执行的时候还是发送一整条语句. PreparedStatementImpl
6. 简单实现拦截器,拦截sql语句的执行.






