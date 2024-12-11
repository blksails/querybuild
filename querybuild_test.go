package querybuild

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// TestUser 测试用户模型
type TestUser struct {
	ID        uint      `gorm:"primarykey"`
	Name      string    `gorm:"column:name"`
	Email     string    `gorm:"column:email"`
	Age       int       `gorm:"column:age"`
	Status    string    `gorm:"column:status"`
	Tags      string    `gorm:"column:tags"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func setupTestDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)

	// 创建表
	err = db.AutoMigrate(&TestUser{})
	assert.NoError(t, err)

	// 插入测试数据
	users := []TestUser{
		{Name: "John Doe", Email: "john@example.com", Age: 25, Status: "active", Tags: "tag1,tag2", CreatedAt: time.Now()},
		{Name: "Jane Smith", Email: "jane@example.com", Age: 30, Status: "inactive", Tags: "tag2,tag3", CreatedAt: time.Now().Add(-24 * time.Hour)},
		{Name: "Bob Johnson", Email: "bob@example.com", Age: 35, Status: "active", Tags: "tag1,tag3", CreatedAt: time.Now().Add(-48 * time.Hour)},
	}
	err = db.Create(&users).Error
	assert.NoError(t, err)

	return db
}

func TestQueryBuilder_Basic(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	t.Run("FindAll", func(t *testing.T) {
		var users []TestUser
		req := &FilterRequest{}
		err := builder.FindAll(req, &users)
		assert.NoError(t, err)
		assert.Len(t, users, 3)
	})

	t.Run("FindOne", func(t *testing.T) {
		var user TestUser
		req := &FilterRequest{
			Filters: []Filter{
				{Field: "Name", Op: EQ, Value: "John Doe"},
			},
		}
		err := builder.FindOne(req, &user)
		assert.NoError(t, err)
		assert.Equal(t, "John Doe", user.Name)
	})

	t.Run("Count", func(t *testing.T) {
		req := &FilterRequest{}
		count, err := builder.Count(req)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), count)
	})
}

func TestQueryBuilder_Filters(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	tests := []struct {
		name     string
		filter   Filter
		expected int
		validate func(t *testing.T, users []TestUser)
	}{
		{
			name:     "EQ operator",
			filter:   Filter{Field: "Status", Op: EQ, Value: "active"},
			expected: 2,
		},
		{
			name:     "GT operator",
			filter:   Filter{Field: "Age", Op: GT, Value: "30"},
			expected: 1,
		},
		{
			name:     "LIKE operator",
			filter:   Filter{Field: "Name", Op: LIKE, Value: "John"},
			expected: 2,
			validate: func(t *testing.T, users []TestUser) {
				assert.Equal(t, "John Doe", users[0].Name)
				assert.Equal(t, "Bob Johnson", users[1].Name)
			},
		},
		{
			name:     "CONTAINS operator",
			filter:   Filter{Field: "Name", Op: CONTAINS, Value: "oh"},
			expected: 2,
			validate: func(t *testing.T, users []TestUser) {
				assert.Equal(t, "John Doe", users[0].Name)
				assert.Equal(t, "Bob Johnson", users[1].Name)
			},
		},
		{
			name:     "STARTS_WITH operator",
			filter:   Filter{Field: "Name", Op: STARTS_WITH, Value: "John"},
			expected: 1,
			validate: func(t *testing.T, users []TestUser) {
				assert.Equal(t, "John Doe", users[0].Name)
			},
		},
		{
			name:     "ENDS_WITH operator",
			filter:   Filter{Field: "Name", Op: ENDS_WITH, Value: "son"},
			expected: 1,
			validate: func(t *testing.T, users []TestUser) {
				assert.Equal(t, "Bob Johnson", users[0].Name)
			},
		},
		{
			name:     "IN operator",
			filter:   Filter{Field: "Status", Op: IN, Value: "active,inactive"},
			expected: 3,
		},
		{
			name:     "NOT_NULL operator",
			filter:   Filter{Field: "Email", Op: NOT_NULL},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var users []TestUser
			req := &FilterRequest{
				Filters: []Filter{tt.filter},
			}
			err := builder.FindAll(req, &users)
			assert.NoError(t, err)
			assert.Len(t, users, tt.expected)
			if tt.validate != nil {
				tt.validate(t, users)
			}
		})
	}
}

func TestQueryBuilder_Sort(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	t.Run("Sort by age DESC", func(t *testing.T) {
		var users []TestUser
		req := &FilterRequest{
			Sorts: []Sort{
				{Field: "Age", Desc: true},
			},
		}
		err := builder.FindAll(req, &users)
		assert.NoError(t, err)
		assert.Len(t, users, 3)
		assert.Equal(t, 35, users[0].Age)
		assert.Equal(t, 25, users[2].Age)
	})
}

func TestQueryBuilder_Pagination(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	t.Run("Page 1 with size 2", func(t *testing.T) {
		var users []TestUser
		req := &FilterRequest{
			Page: &Pagination{
				Page:     1,
				PageSize: 2,
			},
		}
		err := builder.FindAll(req, &users)
		assert.NoError(t, err)
		assert.Len(t, users, 2)
		assert.Equal(t, int64(3), req.Page.Total)
	})
}

func TestQueryBuilder_CustomScope(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	// 注册自定义作用域
	builder.RegisterScope(FilterScope, "activeUsers", func(db *gorm.DB) *gorm.DB {
		return db.Where("status = ?", "active")
	})

	t.Run("Custom scope filter", func(t *testing.T) {
		var users []TestUser
		req := &FilterRequest{
			CustomFilter: &CustomFilter{
				ScopeName: "activeUsers",
			},
		}
		err := builder.FindAll(req, &users)
		assert.NoError(t, err)
		assert.Len(t, users, 2)
		for _, user := range users {
			assert.Equal(t, "active", user.Status)
		}
	})
}

func TestQueryBuilder_Aggregation(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	t.Run("AVG without alias", func(t *testing.T) {
		type Result struct {
			Age float64 `gorm:"column:age"` // 使用原字段名
		}
		var result Result
		req := &FilterRequest{
			Aggrs: []Aggregation{
				{Field: "Age", Op: AVG}, // 不设置别名
			},
		}
		err := builder.FindOne(req, &result)
		assert.NoError(t, err)
		assert.Equal(t, float64(30), result.Age)
	})

	t.Run("AVG with custom alias", func(t *testing.T) {
		type Result struct {
			AverageAge float64 `gorm:"column:average_age"`
		}
		var result Result
		req := &FilterRequest{
			Aggrs: []Aggregation{
				{Field: "Age", Op: AVG, Alias: "average_age"},
			},
		}
		err := builder.FindOne(req, &result)
		assert.NoError(t, err)
		assert.Equal(t, float64(30), result.AverageAge)
	})

	t.Run("Multiple aggregations with mixed alias usage", func(t *testing.T) {
		type Result struct {
			Age       float64 `gorm:"column:age"`        // 原字段名
			UserCount int64   `gorm:"column:user_count"` // 自定义别名
			MaxAge    int     `gorm:"column:max_age"`    // 自定义别名
		}
		var result Result
		req := &FilterRequest{
			Aggrs: []Aggregation{
				{Field: "Age", Op: AVG},                       // 不使用别名
				{Field: "ID", Op: COUNT, Alias: "user_count"}, // 使用别名
				{Field: "Age", Op: MAX, Alias: "max_age"},     // 使用别名
			},
		}
		err := builder.FindOne(req, &result)
		assert.NoError(t, err)
		assert.Equal(t, float64(30), result.Age)
		assert.Equal(t, int64(3), result.UserCount)
		assert.Equal(t, 35, result.MaxAge)
	})

	t.Run("Case insensitive aggregation", func(t *testing.T) {
		type Result struct {
			Status int64 `gorm:"column:status"` // 使用原字段名
		}
		var result Result
		req := &FilterRequest{
			Aggrs: []Aggregation{
				{
					Field:  "Status",
					Op:     COUNT,
					NoCase: true,
				},
			},
			Filters: []Filter{
				{
					Field:  "Status",
					Op:     EQ,
					Value:  "ACTIVE",
					NoCase: true,
				},
			},
		}
		err := builder.FindOne(req, &result)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), result.Status)
	})
}

func TestQueryBuilder_InvalidField(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	t.Run("Invalid field name", func(t *testing.T) {
		var users []TestUser
		req := &FilterRequest{
			Filters: []Filter{
				{Field: "InvalidField", Op: EQ, Value: "test"},
			},
		}
		err := builder.FindAll(req, &users)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid field name")
	})
}

func TestOperator_String(t *testing.T) {
	tests := []struct {
		op       Operator
		expected string
	}{
		{EQ, "EQ"},
		{NE, "NE"},
		{GT, "GT"},
		{LIKE, "LIKE"},
		{IN, "IN"},
		{IS_NULL, "IS_NULL"},
		{STARTS_WITH, "STARTS_WITH"},
		{Operator(999), "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.op.String())
		})
	}
}

func TestQueryBuilder_Count(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	tests := []struct {
		name     string
		request  *FilterRequest
		expected int64
	}{
		{
			name:     "Count all",
			request:  &FilterRequest{},
			expected: 3,
		},
		{
			name: "Count with filter",
			request: &FilterRequest{
				Filters: []Filter{
					{Field: "Status", Op: EQ, Value: "active"},
				},
			},
			expected: 2,
		},
		{
			name: "Count with multiple filters",
			request: &FilterRequest{
				Filters: []Filter{
					{Field: "Status", Op: EQ, Value: "active"},
					{Field: "Age", Op: GT, Value: "30"},
				},
			},
			expected: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			count, err := builder.Count(tt.request)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, count)
		})
	}
}

// 添加一个测试以验证表名设置是否正确
func TestQueryBuilder_TableName(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	t.Run("Check table name", func(t *testing.T) {
		query := builder.Build(&FilterRequest{})
		// 验证生成的SQL中包含正确的表名
		sql := query.Statement.SQL.String()
		assert.Contains(t, sql, "test_users")
	})
}

func TestQueryBuilder_Group(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	// 插入更多测试数据以便测试分组
	additionalUsers := []TestUser{
		{Name: "Alice", Email: "alice@example.com", Age: 25, Status: "active", Tags: "tag1,tag4"},
		{Name: "Charlie", Email: "charlie@example.com", Age: 30, Status: "inactive", Tags: "tag2,tag4"},
	}
	err := db.Create(&additionalUsers).Error
	assert.NoError(t, err)

	t.Run("Group by status", func(t *testing.T) {
		type Result struct {
			Status string `gorm:"column:status"`
			Count  int64  `gorm:"column:count"`
		}
		var results []Result
		req := &FilterRequest{
			Groups: []Group{
				{Field: "Status"},
			},
			Aggrs: []Aggregation{
				{Field: "ID", Op: COUNT, Alias: "count"},
			},
		}
		err := builder.FindAll(req, &results)
		assert.NoError(t, err)
		assert.Len(t, results, 2)
		for _, r := range results {
			switch r.Status {
			case "active":
				assert.Equal(t, int64(3), r.Count)
			case "inactive":
				assert.Equal(t, int64(2), r.Count)
			}
		}
	})

	t.Run("Group by status and age", func(t *testing.T) {
		type Result struct {
			Status    string  `gorm:"column:status"`
			Age       int     `gorm:"column:age"`
			AvgAge    float64 `gorm:"column:age_avg"`
			UserCount int64   `gorm:"column:user_count"`
		}
		var results []Result
		req := &FilterRequest{
			Groups: []Group{
				{Field: "Status"},
				{Field: "Age"},
			},
			Aggrs: []Aggregation{
				{Field: "Age", Op: AVG, Alias: "age_avg"},
				{Field: "ID", Op: COUNT, Alias: "user_count"},
			},
			Sorts: []Sort{
				{Field: "Status", Desc: false},
				{Field: "Age", Desc: true},
			},
		}
		err := builder.FindAll(req, &results)
		assert.NoError(t, err)
		assert.NotEmpty(t, results)

		// 验证分组结果
		statusGroups := make(map[string]int)
		for _, r := range results {
			statusGroups[r.Status]++
			assert.Equal(t, float64(r.Age), r.AvgAge) // 单个年龄分组时，平均值等于年龄值
			assert.True(t, r.UserCount > 0)
		}
		assert.True(t, len(statusGroups) == 2) // 应该有两个状态分组
	})

	t.Run("Group with custom scope", func(t *testing.T) {
		// 注册自定义分组作用域
		builder.RegisterScope(GroupScope, "status_group", func(db *gorm.DB) *gorm.DB {
			return db.Group("status").Select("status, COUNT(*) as count")
		})

		type Result struct {
			Status string `gorm:"column:status"`
			Count  int64  `gorm:"column:count"`
		}
		var results []Result
		req := &FilterRequest{
			Groups: []Group{
				{ScopeName: "status_group"},
			},
		}
		err := builder.FindAll(req, &results)
		assert.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("Group with filter", func(t *testing.T) {
		type Result struct {
			Age    int   `gorm:"column:age"`
			Count  int64 `gorm:"column:count"`
			MinAge int   `gorm:"column:min_age"`
			MaxAge int   `gorm:"column:max_age"`
		}
		var results []Result
		req := &FilterRequest{
			Filters: []Filter{
				{Field: "Status", Op: EQ, Value: "active"},
			},
			Groups: []Group{
				{Field: "Age"},
			},
			Aggrs: []Aggregation{
				{Field: "ID", Op: COUNT, Alias: "count"},
				{Field: "Age", Op: MIN, Alias: "min_age"},
				{Field: "Age", Op: MAX, Alias: "max_age"},
			},
			Sorts: []Sort{
				{Field: "Age", Desc: false},
			},
		}
		err := builder.FindAll(req, &results)
		assert.NoError(t, err)
		assert.NotEmpty(t, results)

		// 验证结果
		for _, r := range results {
			assert.Equal(t, r.Age, r.MinAge) // 单个年龄分组时，最小值等于年龄值
			assert.Equal(t, r.Age, r.MaxAge) // 单个年龄分组时，最大值等于年龄值
			assert.True(t, r.Count > 0)
		}
	})

	t.Run("Group with case-insensitive", func(t *testing.T) {
		type Result struct {
			Status string `gorm:"column:status"`
			Count  int64  `gorm:"column:count"`
		}
		var results []Result
		req := &FilterRequest{
			Groups: []Group{
				{Field: "Status"},
			},
			Aggrs: []Aggregation{
				{Field: "ID", Op: COUNT, Alias: "count"},
			},
			Filters: []Filter{
				{Field: "Status", Op: IN, Value: "ACTIVE,active", NoCase: true},
			},
		}
		err := builder.FindAll(req, &results)
		assert.NoError(t, err)
		assert.Len(t, results, 1) // 大小写不敏感时应该只有一个分组
		assert.Equal(t, int64(3), results[0].Count)
	})
}

func TestQueryBuilder_ScopedOperations(t *testing.T) {
	db := setupTestDB(t)
	builder := NewQueryBuilder[TestUser](db)

	// 注册不同类型的作用域
	builder.RegisterScope(FilterScope, "activeUsers", func(db *gorm.DB) *gorm.DB {
		return db.Where("status = ?", "active")
	})

	builder.RegisterScope(SortScope, "nameWithStatus", func(db *gorm.DB) *gorm.DB {
		return db.Order("status ASC, name DESC")
	})

	builder.RegisterScope(GroupScope, "statusWithCount", func(db *gorm.DB) *gorm.DB {
		return db.Group("status").Select("status, COUNT(*) as count")
	})

	builder.RegisterScope(SelectScope, "userInfo", func(db *gorm.DB) *gorm.DB {
		return db.Select("name, email, status")
	})

	builder.RegisterScope(JoinScope, "userTags", func(db *gorm.DB) *gorm.DB {
		return db.Joins("LEFT JOIN user_tags ON user_tags.user_id = test_users.id")
	})

	t.Run("Filter scope", func(t *testing.T) {
		var users []TestUser
		req := &FilterRequest{
			CustomFilter: &CustomFilter{
				ScopeName: "activeUsers",
			},
		}
		err := builder.FindAll(req, &users)
		assert.NoError(t, err)
		for _, user := range users {
			assert.Equal(t, "active", user.Status)
		}
	})

	t.Run("Sort scope", func(t *testing.T) {
		var users []TestUser
		req := &FilterRequest{
			Sorts: []Sort{
				{ScopeName: "nameWithStatus"},
			},
		}
		err := builder.FindAll(req, &users)
		assert.NoError(t, err)
		assert.NotEmpty(t, users)
	})

	t.Run("Group scope", func(t *testing.T) {
		type Result struct {
			Status string `gorm:"column:status"`
			Count  int64  `gorm:"column:count"`
		}
		var results []Result
		req := &FilterRequest{
			Groups: []Group{
				{ScopeName: "statusWithCount"},
			},
		}
		err := builder.FindAll(req, &results)
		assert.NoError(t, err)
		assert.Len(t, results, 2)
	})

	t.Run("Select scope", func(t *testing.T) {
		var users []TestUser
		req := &FilterRequest{
			CustomFields: []CustomField{
				{ScopeName: "userInfo"},
			},
		}
		err := builder.FindAll(req, &users)
		assert.NoError(t, err)
		assert.NotEmpty(t, users)
	})

	t.Run("Combined scopes", func(t *testing.T) {
		type Result struct {
			Status string `gorm:"column:status"`
			Count  int64  `gorm:"column:count"`
		}
		var results []Result
		req := &FilterRequest{
			CustomFilter: &CustomFilter{
				ScopeName: "activeUsers",
			},
			Groups: []Group{
				{ScopeName: "statusWithCount"},
			},
		}
		err := builder.FindAll(req, &results)
		assert.NoError(t, err)
		assert.Len(t, results, 1)
		assert.Equal(t, "active", results[0].Status)
	})
}
