package acl

import "github.com/go-zookeeper/zk"

const (
	zkUser     = "keeper"
	zkPassword = "admin"
)

func GetKeeperACLandAuthForZK() ([]zk.ACL, string, []byte) {
	acls := zk.WorldACL(zk.PermRead)
	digest := zk.DigestACL(zk.PermAll, zkUser, zkPassword)
	acls = append(acls, digest...)
	return acls, "digest", []byte(zkUser + ":" + zkPassword)
}
