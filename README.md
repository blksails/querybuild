# QueryBuilder

QueryBuilder 是一个基于 GORM 的灵活查询构建器，支持过滤、排序、分组、聚合等复杂查询操作。

## 特性

- 类型安全的泛型实现
- 支持多种过滤操作符
- 支持字段别名和自定义表达式
- 支持分组和聚合查询
- 支持分页
- 支持自定义查询作用域
- 防止 SQL 注入

## 安装
```bash
go get pkg.blksails.net/x/querybuild
```
## 基本用法

```go
// 创建查询构建器
builder := querybuild.NewQueryBuilderUser
// 基本查询
req := &querybuild.FilterRequest{
    Filters: []querybuild.Filter{
        {Field: "Status", Op: querybuild.EQ, Value: "active"},
        {Field: "Age", Op: querybuild.GT, Value: "18"},
    },
    Sorts: []querybuild.Sort{
        {Field: "CreatedAt", Desc: true},
    },
}
var users []User
if err := builder.FindAll(req, &users); err != nil {
// 处理错误
}
```
## 高级用法
### 聚合查询
```go
type Result struct {
    Status string gorm:"column:status"
    AvgAge float64 gorm:"column:avg_age"
    UserCount int64 gorm:"column:user_count"
}

eq := &querybuild.FilterRequest{
    Groups: []querybuild.Group{
        {Field: "Status"},
    },
    Aggrs: []querybuild.Aggregation{
        {Field: "Age", Op: querybuild.AVG, Alias: "avg_age"},
        {Field: "ID", Op: querybuild.COUNT, Alias: "user_count"},
    },
}
var results []Result
builder.FindAll(req, &results)
```
### 自定义作用域
```go
// 注册作用域
builder.RegisterScope(querybuild.FilterScope, "activeAdults", func(db gorm.DB) gorm.DB {
    return db.Where("status = ? AND age >= ?", "active", 18)
})
// 使用作用域
req := &querybuild.FilterRequest{
    CustomFilter: &querybuild.CustomFilter{
        ScopeName: "activeAdults",
    },
}
```
### 支持的操作符
- EQ: 等于
- NE: 不等于
- GT: 大于
- GE: 大于等于
- LT: 小于
- LE: 小于等于
- LIKE: 模糊匹配
- IN: 包含于
- NOT_IN: 不包含于
- BETWEEN: 区间
- IS_NULL: 为空
- NOT_NULL: 非空
- STARTS_WITH: 以...开始
- ENDS_WITH: 以...结束
- CONTAINS: 包含
- NOT_LIKE: 不匹配
- REGEXP: 正则匹配
- NOT_REGEXP: 正则不匹配


### 作用域类型

- FilterScope: 过滤条件作用域
- SortScope: 排序作用域
- GroupScope: 分组作用域
- SelectScope: 字段选择作用域
- JoinScope: 连接作用域

## 注意事项

1. 字段名验证：所有字段名都会经过验证，确保安全性
2. 自定义表达式：复杂的SQL表达式应通过作用域实现
3. 分页：当使用分页时会自动计算总记录数
4. 大小写敏感：支持通过 NoCase 选项进行大小写不敏感的查询
5. 性能考虑：合理使用索引以提高查询性能

## 许可证

MIT License