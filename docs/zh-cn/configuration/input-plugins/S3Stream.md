## Input plugin : S3Stream [Streaming]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

从S3云存储上读取原始数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [path](#path-string) | string | yes | - |

##### path [string]

S3云存储路径，当前支持的路径格式有**s3://**, **s3a://**, **s3n://**

### Example

```
s3Stream {
    path = "s3n://bucket/access.log"
}
```