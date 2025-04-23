package security

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/seaweedfs/seaweedfs/weed/glog"
)

var (
	ErrUnauthorized = errors.New("unauthorized token")
)

type Permission struct {
	Bucket    string   `json:"bucket"`
	Scope     string   `json:"scope"`
	scopeList []string // 内部使用，解析后的权限列表
}

type User struct {
	Username    string       `json:"username"`
	Password    string       `json:"password"`
	Permissions []Permission `json:"permissions"`
}

type UserConfig struct {
	Users []User `json:"users"`
}

type Guard struct {
	whiteList           []string
	SigningKey          SigningKey
	ExpiresAfterSec     int
	ReadSigningKey      SigningKey
	ReadExpiresAfterSec int
	username            string
	password            string
	userConfig          *UserConfig
	usersFile           string

	isBasicAuth   bool
	isWriteActive bool
}

func LoadUserConfig(filePath string) (*UserConfig, error) {
	if filePath == "" {
		return nil, nil
	}
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read user config file: %v", err)
	}
	var config UserConfig
	if err := json.Unmarshal(data, &config.Users); err != nil {
		return nil, fmt.Errorf("failed to parse user config: %v", err)
	}
	// 预处理权限列表
	for i := range config.Users {
		for j := range config.Users[i].Permissions {
			config.Users[i].Permissions[j].scopeList = strings.Split(config.Users[i].Permissions[j].Scope, ",")
		}
	}
	glog.V(0).Infof("Loaded user config from %s: %d users: %+v", filePath, len(config.Users), config.Users)
	return &config, nil
}

func NewGuard(whiteList []string, signingKey string, expiresAfterSec int, readSigningKey string, readExpiresAfterSec int, username string, password string, usersFile string) *Guard {
	userConfig, err := LoadUserConfig(usersFile)
	if err != nil {
		glog.Errorf("Failed to load user config: %v", err)
	}

	g := &Guard{
		whiteList:           whiteList,
		SigningKey:          SigningKey(signingKey),
		ExpiresAfterSec:     expiresAfterSec,
		ReadSigningKey:      SigningKey(readSigningKey),
		ReadExpiresAfterSec: readExpiresAfterSec,
		username:            username,
		password:            password,
		userConfig:          userConfig,
		usersFile:           usersFile,
	}
	g.isWriteActive = len(g.whiteList) != 0 || len(g.SigningKey) != 0
	// 只有当单用户认证或多用户认证至少有一个配置时，才启用认证
	g.isBasicAuth = (g.username != "" && g.password != "") || (g.userConfig != nil && len(g.userConfig.Users) > 0)
	// 如果配置了用户文件，则定时重新加载
	if usersFile != "" {
		go func(path string) {
			ticker := time.NewTicker(time.Minute)
			for range ticker.C {
				cfg, err := LoadUserConfig(path)
				if err != nil {
					glog.Errorf("Failed to reload user config: %v", err)
					continue
				}
				g.userConfig = cfg
				glog.V(0).Infof("Reloaded user config from %s: %d users", path, len(cfg.Users))
			}
		}(usersFile)
	}
	return g
}

func (g *Guard) WhiteList(f http.HandlerFunc) http.HandlerFunc {
	basicAuthFunc := g.BasicAuth(f)
	if !g.isWriteActive {
		//if no security needed, just skip all checking
		return basicAuthFunc
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if err := g.checkWhiteList(w, r); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		basicAuthFunc(w, r)
	}
}

func (g *Guard) checkUserPermission(username string, bucket string, operation string) bool {
	if g.userConfig == nil {
		return false
	}
	for _, user := range g.userConfig.Users {
		if user.Username == username {
			for _, perm := range user.Permissions {
				if perm.Bucket == "" || perm.Bucket == bucket {
					for _, scope := range perm.scopeList {
						if scope == operation {
							return true
						}
					}
				}
			}
			break
		}
	}
	return false
}

func (g *Guard) BasicAuth(f http.HandlerFunc) http.HandlerFunc {
	if !g.isBasicAuth {
		return f
	}
	return func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// 先检查单用户模式
		if g.username != "" && g.password != "" {
			if username == g.username && password == g.password {
				f(w, r)
				return
			}
		}

		// 检查多用户模式
		if g.userConfig != nil && len(g.userConfig.Users) > 0 {
			for _, user := range g.userConfig.Users {
				if username == user.Username && password == user.Password {
					// 获取请求的 bucket 和操作类型
					bucket := r.URL.Query().Get("bucket")
					operation := "read" // 默认为读操作
					if r.Method == "POST" || r.Method == "PUT" {
						operation = "write"
					} else if r.Method == "DELETE" {
						operation = "delete"
					}

					if g.checkUserPermission(username, bucket, operation) {
						f(w, r)
						return
					}
				}
			}
		}

		w.WriteHeader(http.StatusUnauthorized)
	}
}

func GetActualRemoteHost(r *http.Request) (host string, err error) {
	host = r.Header.Get("HTTP_X_FORWARDED_FOR")
	if host == "" {
		host = r.Header.Get("X-FORWARDED-FOR")
	}
	if strings.Contains(host, ",") {
		host = host[0:strings.Index(host, ",")]
	}
	if host == "" {
		host, _, err = net.SplitHostPort(r.RemoteAddr)
	}
	return
}

func (g *Guard) checkWhiteList(w http.ResponseWriter, r *http.Request) error {
	if len(g.whiteList) == 0 {
		return nil
	}

	host, err := GetActualRemoteHost(r)
	if err == nil {
		for _, ip := range g.whiteList {

			// If the whitelist entry contains a "/" it
			// is a CIDR range, and we should check the
			// remote host is within it
			if strings.Contains(ip, "/") {
				_, cidrnet, err := net.ParseCIDR(ip)
				if err != nil {
					panic(err)
				}
				remote := net.ParseIP(host)
				if cidrnet.Contains(remote) {
					return nil
				}
			}

			//
			// Otherwise we're looking for a literal match.
			//
			if ip == host {
				return nil
			}
		}
	}

	glog.V(0).Infof("Not in whitelist: %s", r.RemoteAddr)
	return fmt.Errorf("Not in whitelist: %s", r.RemoteAddr)
}
