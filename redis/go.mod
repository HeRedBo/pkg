module github.com/HeRedBo/pkg/redis

go 1.24

require (
	github.com/HeRedBo/pkg/compression v1.0.0
	github.com/gookit/goutil v0.6.18
	github.com/redis/go-redis/v9 v9.19.0
)

//replace pkg/compression => ../compression // 关键：映射到本地的 compression 目录

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/gookit/color v1.5.4 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
)
