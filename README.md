FlinkX
============

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

[English](README_EN.md) | 中文

# 介绍
* **FlinkX是在是袋鼠云内部广泛使用的基于flink的分布式离线和实时的数据同步框架，实现了多种异构数据源之间高效的数据迁移。**

不同的数据源头被抽象成不同的Reader插件，不同的数据目标被抽象成不同的Writer插件。理论上，FlinkX框架可以支持任意数据源类型的数据同步工作。作为一套生态系统，每接入一套新数据源该新加入的数据源即可实现和现有的数据源互通。

<div align=center>
  <img src=docs/images/template.png width=300 />
</div>

FlinkX是一个基于Flink的批流统一的数据同步工具，既可以采集静态的数据，比如MySQL，HDFS等，也可以采集实时变化的数据，比如MySQL binlog，Kafka等。FlinkX目前包含下面这些特性：

- 大部分插件支持并发读写数据，可以大幅度提高读写速度；

- 部分插件支持失败恢复的功能，可以从失败的位置恢复任务，节约运行时间；[失败恢复](docs/restore.md)

- 关系数据库的Reader插件支持间隔轮询功能，可以持续不断的采集变化的数据；[间隔轮询](docs/offline/reader/mysqlreader.md)

- 部分数据库支持开启Kerberos安全认证；[Kerberos](docs/kerberos.md)

- 可以限制reader的读取速度，降低对业务数据库的影响；

- 可以记录writer插件写数据时产生的脏数据；

- 可以限制脏数据的最大数量；

- 支持多种运行模式；

FlinkX目前支持下面这些数据库：

|                        | Database Type  | Reader                          | Writer                          |
|:----------------------:|:--------------:|:-------------------------------:|:-------------------------------:|
| Batch Synchronization  | MySQL          | [doc](docs/offline/reader/mysqlreader.md)        | [doc](docs/offline/writer/mysqlwriter.md)      |
|                        | Oracle         | [doc](docs/offline/reader/oraclereader.md)       | [doc](docs/offline/writer/oraclewriter.md)     |
|                        | SqlServer      | [doc](docs/offline/reader/sqlserverreader.md)    | [doc](docs/offline/writer/sqlserverwriter.md)  |
|                        | PostgreSQL     | [doc](docs/offline/reader/postgresqlreader.md)   | [doc](docs/offline/writer/postgresqlwriter.md) |
|                        | DB2            | [doc](docs/offline/reader/db2reader.md)          | [doc](docs/offline/writer/db2writer.md)        |
|                        | GBase          | [doc](docs/offline/reader/gbasereader.md)        | [doc](docs/offline/writer/gbasewriter.md)      |
|                        | ClickHouse     | [doc](docs/offline/reader/clickhousereader.md)   | [doc](docs/offline/writer/clickhousewriter.md) |
|                        | PolarDB        | [doc](docs/offline/reader/polardbreader.md)      | [doc](docs/offline/writer/polardbwriter.md)    |
|                        | SAP Hana       | [doc](docs/offline/reader/saphanareader.md)      | [doc](docs/offline/writer/saphanawriter.md)    |
|                        | Teradata       | [doc](docs/offline/reader/teradatareader.md)     | [doc](docs/offline/writer/teradatawriter.md)   |
|                        | Phoenix        | [doc](docs/offline/reader/phoenixreader.md)      | [doc](docs/offline/writer/phoenixwriter.md)    |
|                        | 达梦            | [doc](docs/offline/reader/dmreader.md)           | [doc](docs/offline/writer/dmwriter.md)        |
|                        | Greenplum      | [doc](docs/offline/reader/greenplumreader.md)    | [doc](docs/offline/writer/greenplumwriter.md)  |
|                        | KingBase       | [doc](docs/offline/reader/kingbasereader.md)     | [doc](docs/offline/writer/kingbasewriter.md)   |
|                        | Cassandra      | [doc](docs/offline/reader/cassandrareader.md)    | [doc](docs/offline/writer/cassandrawriter.md)  |
|                        | ODPS           | [doc](docs/offline/reader/odpsreader.md)         | [doc](docs/offline/writer/odpswriter.md)       |
|                        | HBase          | [doc](docs/offline/reader/hbasereader.md)        | [doc](docs/offline/writer/hbasewriter.md)      |
|                        | MongoDB        | [doc](docs/offline/reader/mongodbreader.md)      | [doc](docs/offline/writer/mongodbwriter.md)    |
|                        | Kudu           | [doc](docs/offline/reader/kudureader.md)         | [doc](docs/offline/writer/kuduwriter.md)       |
|                        | ElasticSearch  | [doc](docs/offline/reader/esreader.md)           | [doc](docs/offline/writer/eswriter.md)         |
|                        | FTP            | [doc](docs/offline/reader/ftpreader.md)          | [doc](docs/offline/writer/ftpwriter.md)        |
|                        | HDFS           | [doc](docs/offline/reader/hdfsreader.md)         | [doc](docs/offline/writer/hdfswriter.md)       |
|                        | Carbondata     | [doc](docs/offline/reader/carbondatareader.md)   | [doc](docs/offline/writer/carbondatawriter.md) |
|                        | Stream         | [doc](docs/offline/reader/streamreader.md)       | [doc](docs/offline/writer/streamwriter.md) |
|                        | Redis          |                                                  | [doc](docs/offline/writer/rediswriter.md)      |
|                        | Hive           |                                                  | [doc](docs/offline/writer/hivewriter.md)       |
| Stream Synchronization | Kafka          | [doc](docs/realTime/reader/kafkareader.md)       | [doc](docs/realTime/writer/kafkawriter.md)     |
|                        | EMQX           | [doc](docs/realTime/reader/emqxreader.md)        | [doc](docs/realTime/writer/emqxwriter.md)      |
|                        | RestApi        |[doc](docs/realTime/reader/restapireader.md)  | [doc](docs/realTime/writer/restapiwriter.md)   |
|                        | MySQL Binlog   | [doc](docs/realTime/reader/binlogreader.md)      |                                                |
|                        | MongoDB Oplog  | [doc](docs/realTime/reader/mongodboplogreader.md)|                                                |
|                        | PostgreSQL WAL | [doc](docs/realTime/reader/pgwalreader.md)       |                                                |
|                        | Oracle LogMiner  | [doc](docs/realTime/reader/LogMiner.md)      |                                                |
|                        | Sqlserver CDC  | [doc](docs/realTime/reader/sqlservercdc.md)      |                                                |

# 基本原理
在底层实现上，FlinkX依赖Flink，数据同步任务会被翻译成StreamGraph在Flink上执行，基本原理如下图：
<div align=center>
  <img src=docs/images/diagram.png width=700 />
</div>

# Changelog

请点击[变更日志](docs/changelog.md)

# 快速开始

请点击[快速开始](docs/quickstart.md)

# 通用配置

请点击[插件通用配置](docs/generalconfig.md)

# 统计指标

请点击[统计指标](docs/statistics.md)

# Kerberos

请点击[Kerberos](docs/kerberos.md)

# Questions

请点击[Questions](docs/questions.md)

# 如何贡献FlinkX

请点击[如何贡献FlinkX](docs/contribution.md)

# License

FlinkX is under the Apache 2.0 license. See the [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) file for details.
