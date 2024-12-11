package querybuild_test

import (
	"fmt"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"pkg.blksails.net/x/querybuild"
)

// User 示例用户模型
type User struct {
	ID        uint      `gorm:"primarykey"`
	Name      string    `gorm:"column:name"`
	Email     string    `gorm:"column:email"`
	Age       int       `gorm:"column:age"`
	Status    string    `gorm:"column:status"`
	Tags      string    `gorm:"column:tags"`
	CreatedAt time.Time `gorm:"column:created_at"`
}

func Example_basic() {
	// 初始化数据库连接
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	db.AutoMigrate(&User{})

	// 创建查询构建器
	builder := querybuild.NewQueryBuilder[User](db)

	// 基本查询示例
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
		fmt.Printf("Query error: %v\n", err)
		return
	}

	fmt.Printf("Found %d users\n", len(users))
}

func Example_aggregation() {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	builder := querybuild.NewQueryBuilder[User](db)

	// 聚合查询示例
	type Result struct {
		Status    string  `gorm:"column:status"`
		AvgAge    float64 `gorm:"column:avg_age"`
		UserCount int64   `gorm:"column:user_count"`
	}

	req := &querybuild.FilterRequest{
		Groups: []querybuild.Group{
			{Field: "Status"},
		},
		Aggrs: []querybuild.Aggregation{
			{Field: "Age", Op: querybuild.AVG, Alias: "avg_age"},
			{Field: "ID", Op: querybuild.COUNT, Alias: "user_count"},
		},
	}

	var results []Result
	if err := builder.FindAll(req, &results); err != nil {
		fmt.Printf("Query error: %v\n", err)
		return
	}
}

func Example_customScope() {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	builder := querybuild.NewQueryBuilder[User](db)

	// 注册自定义作用域
	builder.RegisterScope(querybuild.FilterScope, "activeAdults", func(db *gorm.DB) *gorm.DB {
		return db.Where("status = ? AND age >= ?", "active", 18)
	})

	builder.RegisterScope(querybuild.SortScope, "nameWithStatus", func(db *gorm.DB) *gorm.DB {
		return db.Order("status ASC, name DESC")
	})

	// 使用自定义作用域
	req := &querybuild.FilterRequest{
		CustomFilter: &querybuild.CustomFilter{
			ScopeName: "activeAdults",
		},
		Sorts: []querybuild.Sort{
			{ScopeName: "nameWithStatus"},
		},
	}

	var users []User
	if err := builder.FindAll(req, &users); err != nil {
		fmt.Printf("Query error: %v\n", err)
		return
	}
}

func Example_complexQuery() {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	builder := querybuild.NewQueryBuilder[User](db)

	// 复杂查询示例
	type Result struct {
		Status    string  `gorm:"column:status"`
		AgeGroup  int     `gorm:"column:age_group"`
		AvgAge    float64 `gorm:"column:avg_age"`
		UserCount int64   `gorm:"column:user_count"`
	}

	// 注册自定义分组作用域
	builder.RegisterScope(querybuild.GroupScope, "ageGroups", func(db *gorm.DB) *gorm.DB {
		return db.Select(`
			status,
			FLOOR(age/10)*10 as age_group,
			AVG(age) as avg_age,
			COUNT(*) as user_count
		`).Group("status, age_group")
	})

	req := &querybuild.FilterRequest{
		Filters: []querybuild.Filter{
			{Field: "Age", Op: querybuild.GT, Value: "20"},
			{Field: "Status", Op: querybuild.IN, Value: "active,pending"},
		},
		Groups: []querybuild.Group{
			{ScopeName: "ageGroups"},
		},
		Sorts: []querybuild.Sort{
			{Field: "Status", Desc: false},
			{Field: "AgeGroup", Desc: true},
		},
		Page: &querybuild.Pagination{
			Page:     1,
			PageSize: 10,
		},
	}

	var results []Result
	if err := builder.FindAll(req, &results); err != nil {
		fmt.Printf("Query error: %v\n", err)
		return
	}
}
