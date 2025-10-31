package observer

import (
	"git.ztosys.com/ZTO_CS/zcat-go-sdk/zcat"
	"os"
)

var (
	zcatEnabledKey = "WEED_ZCAT_ENABLED"
	envKey         = "WEED_ZCAT_ENV"
	groupKey       = "WEED_ZCAT_GROUP"

	serverNamePrefix = "seaweedfs"
	cmdList          = map[string]struct{}{
		"server": {}, "volume": {}, "master": {},
		"filer": {}, "shell": {},
	}
)

func InitZcat(subCmd string) {
	if _, ok := cmdList[subCmd]; !ok {
		return
	}
	zcatEnabled := os.Getenv(zcatEnabledKey)
	if zcatEnabled == "true" {
		env := os.Getenv(envKey)
		zcatEnv := zcat.NewEnv(env)
		if zcatEnv != zcat.FAT && zcatEnv != zcat.PRO && zcatEnv != zcat.SIT && zcatEnv != zcat.DEV {
			panic("invalid zcat env:" + env)
		}
		group := os.Getenv(groupKey)

		var seaweedGroup string
		if group == "" {
			seaweedGroup = serverNamePrefix
		} else {
			seaweedGroup = serverNamePrefix + group
		}
		zcat.Init(seaweedGroup, zcatEnv)
	}
}

func InitTrans(mType, name string) zcat.Transactor {
	spanContext := zcat.ChildOfSpanContext(zcat.EmptySpanContext)
	return zcat.StartTransactorWithSpanContext(spanContext, mType, name)
}
