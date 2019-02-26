module github.com/go-ego/riot

require (
	github.com/AndreasBriese/bbloom v0.0.0-20180913140656-343706a395b7 // indirect
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	// github.com/coreos/etcd v3.3.10+incompatible // indirect
	// github.com/coreos/bbolt v1.3.0
	// github.com/coreos/etcd v3.3.9+incompatible
	github.com/dgraph-io/badger v1.5.4
	github.com/dgryski/go-farm v0.0.0-20180109070241-2de33835d102 // indirect
	github.com/go-ego/gpy v0.0.0-20181128170341-b6d42325845c
	github.com/go-ego/gse v0.0.0-20190224123304-4377e92184ac
	github.com/go-ego/murmur v0.0.0-20181129155752-fac557227e04
	github.com/go-vgo/gt v0.0.0-20181207163017-e40d098f9006
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/onsi/ginkgo v1.7.0 // indirect
	github.com/onsi/gomega v1.4.3 // indirect
	github.com/pkg/errors v0.8.0 // indirect
	github.com/shirou/gopsutil v2.18.12+incompatible
	github.com/stretchr/testify v1.3.0 // indirect
	github.com/syndtr/goleveldb v0.0.0-20181128100959-b001fa50d6b2
	// not github
	go.etcd.io/bbolt v1.3.1-etcd.7
	// golang.org/x/lint v0.0.0-20181026193005-c67002cb31c3 // indirect
	golang.org/x/net v0.0.0-20181207154023-610586996380 // indirect
	golang.org/x/sync v0.0.0-20181108010431-42b317875d0f // indirect
	golang.org/x/sys v0.0.0-20181210030007-2a47403f2ae5 // indirect
	gopkg.in/yaml.v2 v2.2.2 // indirect
// honnef.co/go/tools v0.0.0-20180920025451-e3ad64cb4ed3 // indirect
)

replace (
	go.etcd.io/bbolt v1.3.1-etcd.7 => github.com/etcd-io/bbolt v1.3.1-etcd.7
	go.etcd.io/etcd v3.3.10+incompatible => github.com/etcd-io/etcd v3.3.10+incompatible
	golang.org/x/net v0.0.0-20181114220301-adae6a3d119a => github.com/golang/net v0.0.0-20181114220301-adae6a3d119a
	golang.org/x/sys v0.0.0-20181122145206-62eef0e2fa9b => github.com/golang/sys v0.0.0-20181122145206-62eef0e2fa9b
	golang.org/x/text v0.3.0 => github.com/golang/text v0.3.0
	google.golang.org/grpc v1.17.0 => github.com/grpc/grpc-go v1.17.0
)
