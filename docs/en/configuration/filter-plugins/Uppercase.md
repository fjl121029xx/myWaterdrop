## Filter plugin : Uppercase

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Uppercase specified field.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | \_\_root\_\_ |

##### source_field [string]

Source field, default is `raw_message`

##### target_field [string]

New field name, default is `__root__`

### Example

```
uppercase {
    source_field = "username"
}
```