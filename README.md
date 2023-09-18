# Lealone-DocDB

Lealone-DocDB 是一个兼容 [MongoDB](https://www.mongodb.com/) 的文档数据库

当前项目处于原型开发阶段


## 编译需要

* Git 2.32+
* JDK 17+
* Maven 3.8+


## 下载项目源代码

`git clone https://github.com/lealone/Lealone-DocDB.git lealone-docdb`

假设源代码放在 `E:\lealone-docdb`


## 从源代码构建

进入 E:\lealone-docdb 目录，运行: `mvn clean package assembly:assembly -Dmaven.test.skip=true`

生成的文件放在 E:\lealone-docdb\target 目录中，
默认生成 docdb-x.y.z.tar.gz 和 docdb-x.y.z.zip 两个压缩文件，
其中 x.y.z 代表实际的版本号

如果运行: `mvn package -Dmaven.test.skip=true -P database`

只生成 docdb-x.y.z.jar


## 运行 Lealone-DocDB

进入 `target\docdb-5.2.0\bin` 目录，运行: `docdb`

或者进入 `target` 目录，运行: `java -jar docdb-5.2.0.jar`

```java
INFO 00:23:02.174 Lealone version: 5.2.0
INFO 00:23:02.180 Loading config from file:/E:/lealone-docdb/target/docdb-5.2.0/conf/docdb.yaml
INFO 00:23:02.241 Base dir: E:/lealone-docdb/target/docdb-5.2.0/data
INFO 00:23:02.246 Init storage engines: 2 ms
INFO 00:23:02.272 Init transaction engines: 25 ms
INFO 00:23:02.275 Init sql engines: 2 ms
INFO 00:23:02.453 Init protocol server engines: 177 ms
INFO 00:23:02.454 Init lealone database: 0 ms
INFO 00:23:02.457 Starting TcpServer accepter
INFO 00:23:02.459 TcpServer started, host: 127.0.0.1, port: 9210
INFO 00:23:02.459 Starting DocDBServer accepter
INFO 00:23:02.460 DocDBServer started, host: 127.0.0.1, port: 9610
INFO 00:23:02.461 Total time: 285 ms (Load config: 65 ms, Init: 214 ms, Start: 6 ms)
INFO 00:23:02.461 Exit with Ctrl+C
```

## 用 MongoDB 客户端访问 Lealone-DocDB

执行以下命令启动 MongoDB 客户端:

`mongosh mongodb://127.0.0.1:9610/mydb?serverSelectionTimeoutMS=200000`

```sql
Connecting to:          mongodb://127.0.0.1:9610/mydb?serverSelectionTimeoutMS=200000
Using MongoDB:          6.0.0
Using Mongosh:          1.9.1

For mongosh info see: https://docs.mongodb.com/mongodb-shell/

mydb> db.runCommand({ insert: "c1", documents: [{ _id: 1, user: "u1", status: "A"}] });
{ ok: 1, n: 1 }
mydb>

mydb> db.runCommand({ find: "c1" });
{
  cursor: {
    id: Long("0"),
    ns: 'mydb.c1',
    firstBatch: [
      { _id: 1, user: 'u1', status: 'A' },
      { _id: 1, user: 'u1', status: 'A' }
    ]
  },
  ok: 1
}

mydb>
```

## 在 IDE 中运行

代码导入 IDE 后，先启动 DocDB Server，直接运行 [DocDBServerStart](https://github.com/lealone/Lealone-DocDB/blob/main/docdb-test/src/test/java/org/lealone/docdb/test/DocDBServerStart.java)

然后测试 CRUD 操作，执行 [DocDBCrudTest](https://github.com/lealone/Lealone-DocDB/blob/main/docdb-test/src/test/java/org/lealone/docdb/test/DocDBCrudTest.java)

