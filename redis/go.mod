module github.com/HeRedBo/pkg/redis

go 1.23.2

require (
	github.com/gookit/goutil v0.6.18
	github.com/redis/go-redis/v9 v9.3.0
	github.com/HeRedBo/pkg/compression v1.0.0  // 使用发布的标签
)

//replace pkg/compression => ../compression // 关键：映射到本地的 compression 目录

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/gookit/color v1.5.4 // indirect
	github.com/xo/terminfo v0.0.0-20220910002029-abceb7e1c41e // indirect
	golang.org/x/sys v0.28.0 // indirect
	golang.org/x/text v0.21.0 // indirect
)
