package querybuild

import (
	"fmt"
	"strings"
	"sync"

	"gorm.io/gorm"
)

// AggregationOp 聚合操作类型
type AggregationOp int32

const (
	UNKNOWN_OP AggregationOp = iota
	COUNT
	SUM
	AVG
	MAX
	MIN
)

// Operator 过滤操作符
type Operator int32

const (
	EQ              Operator = iota // 等于
	NE                              // 不等于
	GT                              // 大于
	GE                              // 大于等于
	LT                              // 小于
	LE                              // 小于等于
	LIKE                            // 模糊匹配
	IN                              // 包含于
	BETWEEN                         // 区间
	NOT_IN                          // 不包含于
	IS_NULL                         // 为空
	NOT_NULL                        // 非空
	STARTS_WITH                     // 以...开始
	ENDS_WITH                       // 以...结束
	CONTAINS                        // 包含
	NOT_LIKE                        // 不匹配
	REGEXP                          // 正则匹配
	NOT_REGEXP                      // 正则不匹配
	OVERLAP                         // 数组重叠
	ARRAY_CONTAINS                  // 数组包含
	ARRAY_CONTAINED                 // 数组被包含
)

// Filter 过滤条件
type Filter struct {
	Field  string   `json:"field"`
	Op     Operator `json:"op"`
	Value  string   `json:"value"`
	NoCase bool     `json:"nocase"`
}

// ScopeType 定义作用域类型
type ScopeType int

const (
	FilterScope ScopeType = iota
	SortScope
	GroupScope
	SelectScope
	JoinScope
)

// ScopeFunc 定义查询作用域函数类型
type ScopeFunc func(db *gorm.DB) *gorm.DB

// ScopeRegistry 作用域函数注册表
type ScopeRegistry struct {
	filterScopes map[string]ScopeFunc // 过滤作用域
	sortScopes   map[string]ScopeFunc // 排序作用域
	groupScopes  map[string]ScopeFunc // 分组作用域
	selectScopes map[string]ScopeFunc // 选择字段作用域
	joinScopes   map[string]ScopeFunc // 连接作用域
	mu           sync.RWMutex
}

// NewScopeRegistry 创建新的作用域注册表
func NewScopeRegistry() *ScopeRegistry {
	return &ScopeRegistry{
		filterScopes: make(map[string]ScopeFunc),
		sortScopes:   make(map[string]ScopeFunc),
		groupScopes:  make(map[string]ScopeFunc),
		selectScopes: make(map[string]ScopeFunc),
		joinScopes:   make(map[string]ScopeFunc),
	}
}

// Register 注册作用域函数
func (r *ScopeRegistry) Register(scopeType ScopeType, name string, scope ScopeFunc) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch scopeType {
	case FilterScope:
		r.filterScopes[name] = scope
	case SortScope:
		r.sortScopes[name] = scope
	case GroupScope:
		r.groupScopes[name] = scope
	case SelectScope:
		r.selectScopes[name] = scope
	case JoinScope:
		r.joinScopes[name] = scope
	}
}

// Get 获取作用域函数
func (r *ScopeRegistry) Get(scopeType ScopeType, name string) (ScopeFunc, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var scope ScopeFunc
	var ok bool

	switch scopeType {
	case FilterScope:
		scope, ok = r.filterScopes[name]
	case SortScope:
		scope, ok = r.sortScopes[name]
	case GroupScope:
		scope, ok = r.groupScopes[name]
	case SelectScope:
		scope, ok = r.selectScopes[name]
	case JoinScope:
		scope, ok = r.joinScopes[name]
	}

	return scope, ok
}

// CustomField 自定义字段定义
type CustomField struct {
	Name      string `json:"name"`  // 字段别名
	ScopeName string `json:"scope"` // 作用域函数名称
}

// CustomFilter 自定义过滤条件
type CustomFilter struct {
	ScopeName string        `json:"scope"`  // 作用域函数名称
	Values    []interface{} `json:"values"` // 条件参数值
}

// Sort 排序条件
type Sort struct {
	Field     string `json:"field"`
	Desc      bool   `json:"desc"`
	NoCase    bool   `json:"nocase"`
	ScopeName string `json:"scope"` // 作用域函数名称
}

// Aggregation 聚合条件
type Aggregation struct {
	Field      string        `json:"field"`
	Op         AggregationOp `json:"op"`
	NoCase     bool          `json:"nocase"`
	AddSelects []string      `json:"add_selects"`
	Alias      string        `json:"alias"` // 聚合结果的别名
}

// Pagination 分页参数
type Pagination struct {
	Page     int   `json:"page"`      // 页码，从1开始
	PageSize int   `json:"page_size"` // 每页数量
	Total    int64 `json:"total"`     // 总记录数
}

// Group 分组条件
type Group struct {
	Field     string `json:"field"`
	Having    string `json:"having"`
	ScopeName string `json:"scope"` // 作用域函数名称
}

// Join 连接条件
type Join struct {
	Type      string `json:"type"`      // LEFT, RIGHT, INNER
	Table     string `json:"table"`     // 要连接的表名
	Condition string `json:"condition"` // 连接条件
	ScopeName string `json:"scope"`     // 作用域函数名称
}

// SubQuery 子查询
type SubQuery struct {
	Field    string        `json:"field"`     // 子查询结果字段名
	Table    string        `json:"table"`     // 子查询表名
	Filter   FilterRequest `json:"filter"`    // 子查询条件
	JoinCond string        `json:"join_cond"` // 与主查询的关联条件
}

// FilterRequest 查询请求
type FilterRequest struct {
	Filters      []Filter      `json:"filters"`
	CustomFields []CustomField `json:"custom_fields"` // 自定义字段
	CustomFilter *CustomFilter `json:"custom_filter"` // 自定义过滤条件
	Sorts        []Sort        `json:"sorts"`
	Aggrs        []Aggregation `json:"aggrs"`
	Page         *Pagination   `json:"page"`
	Groups       []Group       `json:"groups"`
	Joins        []Join        `json:"joins"`
	SubQuery     *SubQuery     `json:"sub_query"`
	Distinct     bool          `json:"distinct"`
}

// FieldInfo 字段信息
type FieldInfo struct {
	Name      string // 数据库字段名
	TableName string // 表名
}

// QueryBuilder GORM查询构建器
type QueryBuilder[T any] struct {
	db       *gorm.DB
	registry *ScopeRegistry
	fields   map[string]FieldInfo // 模型字段映射
	model    T                    // 模型实例
}

// NewQueryBuilder 创建新的查询构建器
func NewQueryBuilder[T any](db *gorm.DB) *QueryBuilder[T] {
	var model T
	qb := &QueryBuilder[T]{
		db:       db,
		registry: NewScopeRegistry(),
		fields:   make(map[string]FieldInfo),
		model:    model,
	}
	qb.initFields()
	return qb
}

// initFields 初始化字段映射
func (qb *QueryBuilder[T]) initFields() {
	var model T
	stmt := &gorm.Statement{DB: qb.db}
	_ = stmt.Parse(&model)

	for _, field := range stmt.Schema.Fields {
		dbName := field.DBName
		if dbName != "" {
			qb.fields[field.Name] = FieldInfo{
				Name:      dbName,
				TableName: stmt.Schema.Table,
			}
		}
	}
}

// validateField 验证字段名是否安全
func (qb *QueryBuilder[T]) validateField(fieldName string) (FieldInfo, error) {
	if info, ok := qb.fields[fieldName]; ok {
		return info, nil
	}
	return FieldInfo{}, fmt.Errorf("invalid field name: %s", fieldName)
}

// safeField 获取安全的字段引用
func (qb *QueryBuilder[T]) safeField(fieldName string) (string, error) {
	info, err := qb.validateField(fieldName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("`%s`.`%s`", info.TableName, info.Name), nil
}

// simpleField 获取简单的字段引用
func (qb *QueryBuilder[T]) simpleField(fieldName string) (string, error) {
	info, err := qb.validateField(fieldName)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("`%s`", info.Name), nil
}

// RegisterScope 注册作用域函数
func (qb *QueryBuilder[T]) RegisterScope(scopeType ScopeType, name string, scope ScopeFunc) {
	qb.registry.Register(scopeType, name, scope)
}

// Build 构建查询
func (qb *QueryBuilder[T]) Build(req *FilterRequest) *gorm.DB {
	// 首先设置模型
	query := qb.db.Model(&qb.model)

	// 应用自定义字段
	query = qb.applyCustomFields(query, req.CustomFields)

	// 应用DISTINCT
	if req.Distinct {
		query = query.Distinct()
	}

	// 应用JOIN
	query = qb.applyJoins(query, req.Joins)

	// 应用子查询
	query = qb.applySubQuery(query, req.SubQuery)

	// 应用标准过滤条件
	query = qb.applyFilters(query, req.Filters)

	// 应用自定义过滤条件
	query = qb.applyCustomFilter(query, req.CustomFilter)

	// 应用分组
	query = qb.applyGroups(query, req.Groups)

	// 应用排序
	query = qb.applySorts(query, req.Sorts)

	// 应用聚合
	query = qb.applyAggregations(query, req.Aggrs)

	// 应用分页
	query = qb.applyPagination(query, req.Page)

	return query
}

// applyFilters 应用过滤条件
func (qb *QueryBuilder[T]) applyFilters(query *gorm.DB, filters []Filter) *gorm.DB {
	for _, filter := range filters {
		safeField, err := qb.safeField(filter.Field)
		if err != nil {
			query.AddError(err)
			continue
		}

		field := safeField
		if filter.NoCase {
			field = fmt.Sprintf("LOWER(%s)", field)
		}

		value := filter.Value
		if filter.NoCase && value != "" {
			value = strings.ToLower(value)
		}

		switch filter.Op {
		case EQ:
			query = query.Where(fmt.Sprintf("%s = ?", field), value)
		case NE:
			query = query.Where(fmt.Sprintf("%s != ?", field), value)
		case GT:
			query = query.Where(fmt.Sprintf("%s > ?", field), value)
		case GE:
			query = query.Where(fmt.Sprintf("%s >= ?", field), value)
		case LT:
			query = query.Where(fmt.Sprintf("%s < ?", field), value)
		case LE:
			query = query.Where(fmt.Sprintf("%s <= ?", field), value)
		case LIKE:
			query = query.Where(fmt.Sprintf("%s LIKE ?", field), "%"+value+"%")
		case IN:
			values := strings.Split(value, ",")
			query = query.Where(fmt.Sprintf("%s IN (?)", field), values)
		case BETWEEN:
			values := strings.Split(value, ",")
			if len(values) == 2 {
				query = query.Where(fmt.Sprintf("%s BETWEEN ? AND ?", field), values[0], values[1])
			}
		case NOT_IN:
			values := strings.Split(value, ",")
			query = query.Where(fmt.Sprintf("%s NOT IN (?)", field), values)
		case IS_NULL:
			query = query.Where(fmt.Sprintf("%s IS NULL", field))
		case NOT_NULL:
			query = query.Where(fmt.Sprintf("%s IS NOT NULL", field))
		case STARTS_WITH:
			query = query.Where(fmt.Sprintf("%s LIKE ?", field), value+"%")
		case ENDS_WITH:
			query = query.Where(fmt.Sprintf("%s LIKE ?", field), "%"+value)
		case CONTAINS:
			query = query.Where(fmt.Sprintf("%s LIKE ?", field), "%"+value+"%")
		case NOT_LIKE:
			query = query.Where(fmt.Sprintf("%s NOT LIKE ?", field), "%"+value+"%")
		case REGEXP:
			query = query.Where(fmt.Sprintf("%s REGEXP ?", field), value)
		case NOT_REGEXP:
			query = query.Where(fmt.Sprintf("%s NOT REGEXP ?", field), value)
		case OVERLAP:
			query = query.Where(fmt.Sprintf("%s && ?", field), value)
		case ARRAY_CONTAINS:
			query = query.Where(fmt.Sprintf("%s @> ?", field), value)
		case ARRAY_CONTAINED:
			query = query.Where(fmt.Sprintf("%s <@ ?", field), value)
		}
	}
	return query
}

// applySorts 应用排序条件
func (qb *QueryBuilder[T]) applySorts(query *gorm.DB, sorts []Sort) *gorm.DB {
	for _, sort := range sorts {
		if sort.ScopeName != "" {
			if scope, ok := qb.registry.Get(SortScope, sort.ScopeName); ok {
				query = scope(query)
				continue
			}
		}

		safeField, err := qb.safeField(sort.Field)
		if err != nil {
			query.AddError(err)
			continue
		}

		field := safeField
		if sort.NoCase {
			field = fmt.Sprintf("LOWER(%s)", field)
		}

		if sort.Desc {
			query = query.Order(fmt.Sprintf("%s DESC", field))
		} else {
			query = query.Order(fmt.Sprintf("%s ASC", field))
		}
	}
	return query
}

// applyAggregations 应用聚合条件
func (qb *QueryBuilder[T]) applyAggregations(query *gorm.DB, aggrs []Aggregation) *gorm.DB {
	if len(aggrs) == 0 {
		return query
	}

	selects := []string{}
	for _, aggr := range aggrs {
		safeField, err := qb.safeField(aggr.Field)
		if err != nil {
			query.AddError(err)
			continue
		}

		field := safeField
		if aggr.NoCase {
			field = fmt.Sprintf("LOWER(%s)", field)
		}

		var expr string
		switch aggr.Op {
		case COUNT:
			expr = fmt.Sprintf("COUNT(%s)", field)
		case SUM:
			expr = fmt.Sprintf("SUM(%s)", field)
		case AVG:
			expr = fmt.Sprintf("AVG(%s)", field)
		case MAX:
			expr = fmt.Sprintf("MAX(%s)", field)
		case MIN:
			expr = fmt.Sprintf("MIN(%s)", field)
		}

		if expr != "" {
			// 如果设置了别名就使用别名，否则使用原字段名
			alias, err := qb.simpleField(aggr.Field)
			if err != nil {
				query.AddError(err)
				continue
			}

			if aggr.Alias != "" {
				alias = aggr.Alias
			}
			expr = fmt.Sprintf("%s as %s", expr, alias)
			selects = append(selects, expr)
		}

		// AddSelects 需要通过 ScopeFunc 来实现以确保安全性
		if len(aggr.AddSelects) > 0 {
			query.AddError(fmt.Errorf("additional selects must be implemented via ScopeFunc"))
		}
	}

	if len(selects) > 0 {
		query = query.Select(strings.Join(selects, ", "))
	}

	return query
}

// applyJoins 应用连接条件
func (qb *QueryBuilder[T]) applyJoins(query *gorm.DB, joins []Join) *gorm.DB {
	for _, join := range joins {
		if join.Type == "" && join.ScopeName != "" {
			if scope, ok := qb.registry.Get(JoinScope, join.ScopeName); ok {
				query = scope(query)
				continue
			}
		}
		switch strings.ToUpper(join.Type) {
		case "LEFT":
			query = query.Joins(fmt.Sprintf("LEFT JOIN %s ON %s", join.Table, join.Condition))
		case "RIGHT":
			query = query.Joins(fmt.Sprintf("RIGHT JOIN %s ON %s", join.Table, join.Condition))
		case "INNER":
			query = query.Joins(fmt.Sprintf("INNER JOIN %s ON %s", join.Table, join.Condition))
		}
	}
	return query
}

// applySubQuery 应用子查询
func (qb *QueryBuilder[T]) applySubQuery(query *gorm.DB, sub *SubQuery) *gorm.DB {
	if sub == nil {
		return query
	}

	subBuilder := NewQueryBuilder[T](qb.db.Table(sub.Table))
	subQuery := subBuilder.Build(&sub.Filter)

	// 将子查询作为派生表
	query = query.Joins(fmt.Sprintf("JOIN (%s) AS %s ON %s",
		subQuery.Statement.SQL.String(),
		sub.Field,
		sub.JoinCond))

	return query
}

// applyGroups 应用分组条件
func (qb *QueryBuilder[T]) applyGroups(query *gorm.DB, groups []Group) *gorm.DB {
	if len(groups) == 0 {
		return query
	}

	groupFields := make([]string, 0, len(groups))
	for _, group := range groups {
		if group.ScopeName != "" {
			if scope, ok := qb.registry.Get(GroupScope, group.ScopeName); ok {
				query = scope(query)
				continue
			}
		}

		safeField, err := qb.safeField(group.Field)
		if err != nil {
			query.AddError(err)
			continue
		}

		groupFields = append(groupFields, safeField)
		if group.Having != "" {
			// Having 条件需要通过 ScopeFunc 来实现以确保安全性
			query.AddError(fmt.Errorf("having conditions must be implemented via ScopeFunc"))
		}
	}

	if len(groupFields) > 0 {
		return query.Group(strings.Join(groupFields, ", "))
	}
	return query
}

// applyPagination 应用分页
func (qb *QueryBuilder[T]) applyPagination(query *gorm.DB, page *Pagination) *gorm.DB {
	if page == nil {
		return query
	}

	// 计算总记录数
	query.Count(&page.Total)

	// 应用分页
	offset := (page.Page - 1) * page.PageSize
	return query.Offset(offset).Limit(page.PageSize)
}

// applyCustomFields 应用自定义字段
func (qb *QueryBuilder[T]) applyCustomFields(query *gorm.DB, fields []CustomField) *gorm.DB {
	if len(fields) == 0 {
		return query
	}

	for _, field := range fields {
		if scope, ok := qb.registry.Get(SelectScope, field.ScopeName); ok {
			query = scope(query)
		}
	}
	return query
}

// applyCustomFilter 应用自定义过滤条件
func (qb *QueryBuilder[T]) applyCustomFilter(query *gorm.DB, filter *CustomFilter) *gorm.DB {
	if filter == nil || filter.ScopeName == "" {
		return query
	}

	if scope, ok := qb.registry.Get(FilterScope, filter.ScopeName); ok {
		query = scope(query)
	}
	return query
}

// Count 获取记录总数
func (qb *QueryBuilder[T]) Count(req *FilterRequest) (int64, error) {
	var count int64
	query := qb.Build(req)
	err := query.Count(&count).Error
	return count, err
}

// FindAll 查询所有记录
func (qb *QueryBuilder[T]) FindAll(req *FilterRequest, dest interface{}) error {
	return qb.Build(req).Find(dest).Error
}

// FindOne 查询单条记录
func (qb *QueryBuilder[T]) FindOne(req *FilterRequest, dest interface{}) error {
	return qb.Build(req).First(dest).Error
}

// 添加操作符的字符串表示方法
func (op Operator) String() string {
	switch op {
	case EQ:
		return "EQ"
	case NE:
		return "NE"
	case GT:
		return "GT"
	case GE:
		return "GE"
	case LT:
		return "LT"
	case LE:
		return "LE"
	case LIKE:
		return "LIKE"
	case IN:
		return "IN"
	case BETWEEN:
		return "BETWEEN"
	case NOT_IN:
		return "NOT_IN"
	case IS_NULL:
		return "IS_NULL"
	case NOT_NULL:
		return "NOT_NULL"
	case STARTS_WITH:
		return "STARTS_WITH"
	case ENDS_WITH:
		return "ENDS_WITH"
	case CONTAINS:
		return "CONTAINS"
	case NOT_LIKE:
		return "NOT_LIKE"
	case REGEXP:
		return "REGEXP"
	case NOT_REGEXP:
		return "NOT_REGEXP"
	case OVERLAP:
		return "OVERLAP"
	case ARRAY_CONTAINS:
		return "ARRAY_CONTAINS"
	case ARRAY_CONTAINED:
		return "ARRAY_CONTAINED"
	default:
		return "UNKNOWN"
	}
}
