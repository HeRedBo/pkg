module pkg/routine

go 1.23.2

require pkg/errors v0.0.0 // 任意版本占位符

require github.com/pkg/errors v0.9.1 // indirect

replace pkg/errors => ../errors // 关键：映射到本地的 errors 目录
