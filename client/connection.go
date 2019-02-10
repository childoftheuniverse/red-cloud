package client

import (
	"crypto/tls"
	"context"
	discovery "github.com/childoftheuniverse/etcd-discovery"
	"github.com/childoftheuniverse/red-cloud/common"
	etcd "go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

/*
GetMasterConnection finds the master for the red-cloud instance associated
with the given path and returns it.
*/
func GetMasterConnection(
	ctx context.Context,
	tlsConfig *tls.Config,
	etcdClient *etcd.Client,
	path string) (*grpc.ClientConn, error) {
	var dialOpts []grpc.DialOption
	var instance string
	var err error

	if instance, _, err = SplitTablePath(path); err != nil {
		return nil, err
	}

	if tlsConfig == nil {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(
			credentials.NewTLS(tlsConfig)))
	}

	return discovery.NewGrpcClient(ctx, etcdClient,
		common.EtcdMasterPrefix(instance), dialOpts...)
}
