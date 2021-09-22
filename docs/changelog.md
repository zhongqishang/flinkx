
## 新增 mongo reader 语法

### `*v*` 
定制格式：配合 kafkawriter 的 tableFields 配置`["_id", "value"]`

Flinkx 任务配置 JSON 文件：
```json
{
	"job": {
		"content": [{
			"reader": {
				"name": "mongodbreader",
				"parameter": {
					"hostPorts": "172.16.188.143:25000",
					"database": "database",
					"username": "username",
					"password": "pwd",
					"collectionName": "test",
					"column": ["*v*"],
					"fetchSize": 0
				}
			},
			"writer": {
				"parameter": {
					"topic": "topic...",
					"producerSettings": {
						"bootstrap.servers": "..."
					},
					"tableFields": ["_id", "value"]
				},
				"name": "kafkawriter"
			}
		}],
		"setting": {
		}
	}
}
```
发送kafka格式：
```json
{
	"_id": "6108b9e061a843c9ca44fa20",
	"value": "{\"age\": 1.0,\"column1\": \"column1_value\"}"
}
```

### `*JSON*`
Flinkx 任务配置 JSON 文件：
```json
{
	"job": {
		"content": [{
			"reader": {
				"name": "mongodbreader",
				"parameter": {
					"hostPorts": "host:port",
					"database": "database",
					"username": "username",
					"password": "pwd",
					"collectionName": "collection",
					"column": ["*JSON*"]
				}
			},
			"writer": {
				"parameter": {
					"path": "hdfs://nn1/user/commondata/dw/flinkx_test_orc",
					"fileName": "dates=20210922",
					"column": [{
						"name": "json_str",
						"index": 0,
						"type": "string"
					}],
					"writeMode": "overwrite",
					"defaultFS": "hdfs://nn1",
					"fileType": "orc"
				},
				"name": "hdfswriter"
			}
		}],
		"setting": {}
	}
}
```
生成一个JSON字段，写入 Hdfs 格式：

```json
{
	"_id": "d5e8a6336a830a8fe66853b96a8fa0ce",
	"FinanceChiefInfo": {
		"FinanceChiefCerNo": "",
		"FinanceChiefGender": "",
		"FinanceChiefName": "",
		"FinanceChiefId": "",
		"FinanceChiefCerType": ""
	},
	"OrgCode": "34257037X",
	"PublishTime": {
		"$numberLong": "1593705600"
	},
	"TaxpayerName": "深圳市达辉明业实业有限公司",
	"TaxpayerNumber": "9144030034257037XA",
	"_CreateTime": "2021-05-13 18:08:04",
	"_DataStatus": null,
	"_UpdateTime": "2021-05-13 18:08:04"
}
```
> 1. 以上是为了方便查看格式化之后的展示，写到 HDFS 是没有格式化的
> 2. "PublishTime" : NumberLong(1593705600)

## 新增 JDBC 切分
支持任意字符切分来增加并发，但是为了避免对源库影响较大，切分键必须为主键或者唯一索引的最左字段。

※ 现支持 MySQL/PG/SQLServer
### 切分规则

### 主键为 int/bigint/numeric 类型
满足条件：计算表的数据分布因子小于 2 的情况会被视为均匀分布，将会采用固定长度(切分大小 = (最大值 - 最小值) / 并发)来切分

```
 (-∞, 25)
 [25, 50)
 [50, +∞)
```

### 主键为其他类型 & 分布因子大于 2
通过 SQL 查询出每个分片的最大最小值来切分，
```
(-∞, 'uuid-001')
['uuid-001', 'uuid-009')
[uuid-009, +∞).
```


> 分布因子 = (max - min + 1) / count
