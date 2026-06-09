module github.com/juicedata/juicefs

go 1.25.0

require (
	cloud.google.com/go/compute/metadata v0.9.0
	cloud.google.com/go/storage v1.48.0
	github.com/Azure/azure-sdk-for-go/sdk/azcore v1.20.0
	github.com/Azure/azure-sdk-for-go/sdk/azidentity v1.13.1
	github.com/Azure/azure-sdk-for-go/sdk/storage/azblob v1.6.4
	github.com/IBM/ibm-cos-sdk-go v1.13.0
	github.com/aliyun/alibabacloud-oss-go-sdk-v2 v1.4.1
	github.com/aliyun/credentials-go v1.4.5
	github.com/aws/aws-sdk-go-v2 v1.41.5
	github.com/aws/aws-sdk-go-v2/config v1.32.14
	github.com/aws/aws-sdk-go-v2/credentials v1.19.14
	github.com/aws/aws-sdk-go-v2/service/s3 v1.99.0
	github.com/aws/smithy-go v1.24.2
	github.com/baidubce/bce-sdk-go v0.9.263
	github.com/ceph/go-ceph v0.18.0
	github.com/cloudsoda/go-smb2 v0.0.0-20250228001242-d4c70e6251cc
	github.com/colinmarc/hdfs/v2 v2.4.0
	github.com/dustin/go-humanize v1.0.1
	github.com/emmansun/gmsm v0.41.1
	github.com/erikdubbelboer/gspt v0.0.0-20210805194459-ce36a5128377
	github.com/go-http-utils/headers v0.0.0-20181008091004-fed159eddc2a
	github.com/go-sql-driver/mysql v1.9.3
	github.com/hashicorp/consul/api v1.29.2
	github.com/hashicorp/go-hclog v1.6.3
	github.com/huaweicloud/huaweicloud-sdk-go-obs v3.21.12+incompatible
	github.com/jackc/pgx/v5 v5.9.2
	github.com/jcmturner/gokrb5/v8 v8.4.4
	github.com/juicedata/gogfapi v0.0.0-20241204082332-ecd102647f80
	github.com/juju/ratelimit v1.0.2
	github.com/ks3sdklib/aws-sdk-go v1.6.0
	github.com/l0wl3vel/bunny-storage-go-sdk v1.0.0
	github.com/mattn/go-isatty v0.0.20
	github.com/mattn/go-sqlite3 v1.14.24
	github.com/ncw/swift/v2 v2.0.3
	github.com/oliverisaac/shellescape v0.0.0-20220131224704-1b6c6b87b668
	github.com/pingcap/log v1.1.1-0.20221110025148-ca232912c9f3
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.10
	github.com/prometheus/client_golang v1.21.1
	github.com/qingstor/qingstor-sdk-go/v4 v4.4.1
	github.com/qiniu/go-sdk/v7 v7.26.8
	github.com/redis/go-redis/v9 v9.18.0
	github.com/sirupsen/logrus v1.9.3
	github.com/spf13/cast v1.7.1
	github.com/stretchr/testify v1.11.1
	github.com/studio-b12/gowebdav v0.10.0
	github.com/tencentyun/cos-go-sdk-v5 v0.7.73
	github.com/tikv/client-go/v2 v2.0.7
	github.com/urfave/cli/v2 v2.19.3
	github.com/vbauerster/mpb/v7 v7.0.3
	github.com/viki-org/dnscache v0.0.0-20130720023526-c70c1f23c5d8
	github.com/vimeo/go-util v1.4.1
	github.com/vmware/go-nfs-client v0.0.0-20190605212624-d43b92724c1b
	github.com/volcengine/ve-tos-golang-sdk/v2 v2.9.0
	go.etcd.io/etcd v3.3.27+incompatible
	go.etcd.io/etcd/client/v3 v3.5.9
	golang.org/x/crypto v0.49.0
	golang.org/x/net v0.52.0
	golang.org/x/oauth2 v0.34.0
	golang.org/x/sys v0.42.0
	golang.org/x/term v0.41.0
	google.golang.org/api v0.210.0
	gopkg.in/kothar/go-backblaze.v0 v0.0.0-20210124194846-35409b867216
	storj.io/uplink v1.14.2
	xorm.io/xorm v1.0.7
)

require (
	cel.dev/expr v0.25.1 // indirect
	cloud.google.com/go v0.116.0 // indirect
	cloud.google.com/go/auth v0.11.0 // indirect
	cloud.google.com/go/auth/oauth2adapt v0.2.6 // indirect
	cloud.google.com/go/iam v1.2.2 // indirect
	cloud.google.com/go/monitoring v1.21.2 // indirect
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/Azure/azure-sdk-for-go/sdk/internal v1.11.2 // indirect
	github.com/AzureAD/microsoft-authentication-library-for-go v1.6.0 // indirect
	github.com/BurntSushi/toml v1.3.2 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/detectors/gcp v1.31.0 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric v0.48.1 // indirect
	github.com/GoogleCloudPlatform/opentelemetry-operations-go/internal/resourcemapping v0.48.1 // indirect
	github.com/IBM/go-sdk-core/v5 v5.21.2 // indirect
	github.com/VividCortex/ewma v1.2.0 // indirect
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d // indirect
	github.com/alex-ant/gomath v0.0.0-20160516115720-89013a210a82 // indirect
	github.com/alibabacloud-go/debug v1.0.1 // indirect
	github.com/alibabacloud-go/tea v1.2.2 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/armon/go-metrics v0.4.1 // indirect
	github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream v1.7.8 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.18.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/configsources v1.4.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/endpoints/v2 v2.7.21 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.8.6 // indirect
	github.com/aws/aws-sdk-go-v2/internal/v4a v1.4.22 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/accept-encoding v1.13.7 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/checksum v1.9.13 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.13.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/s3shared v1.19.21 // indirect
	github.com/aws/aws-sdk-go-v2/service/signin v1.0.9 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.30.15 // indirect
	github.com/aws/aws-sdk-go-v2/service/ssooidc v1.35.19 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.41.10 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/calebcase/tmpfile v1.0.3 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/clbanning/mxj v1.8.4 // indirect
	github.com/cloudsoda/sddl v0.0.0-20250224235906-926454e91efc // indirect
	github.com/cncf/xds/go v0.0.0-20251210132809-ee656c7534f5 // indirect
	github.com/coreos/etcd v3.3.27+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/coreos/pkg v0.0.0-20240122114842-bbd7aa9bf6fb // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgryski/go-farm v0.0.0-20240924180020-3414d57e47da // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/elastic/gosigar v0.14.2 // indirect
	github.com/envoyproxy/go-control-plane/envoy v1.36.0 // indirect
	github.com/envoyproxy/protoc-gen-validate v1.3.0 // indirect
	github.com/fatih/color v1.16.0 // indirect
	github.com/felixge/httpsnoop v1.0.4 // indirect
	github.com/flynn/noise v1.0.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.11 // indirect
	github.com/gammazero/toposort v0.1.1 // indirect
	github.com/geoffgarside/ber v1.1.0 // indirect
	github.com/go-jose/go-jose/v4 v4.1.4 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-openapi/errors v0.22.4 // indirect
	github.com/go-openapi/strfmt v0.25.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.28.0 // indirect
	github.com/go-resty/resty/v2 v2.16.5 // indirect
	github.com/go-viper/mapstructure/v2 v2.4.0 // indirect
	github.com/gofrs/flock v0.8.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.0 // indirect
	github.com/golang/glog v1.2.5 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/btree v1.1.2 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/pprof v0.0.0-20240711041743-f6c9dda6c6da // indirect
	github.com/google/readahead v0.0.0-20161222183148-eaceba169032 // indirect
	github.com/google/s2a-go v0.1.8 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.3.4 // indirect
	github.com/googleapis/gax-go/v2 v2.14.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-retryablehttp v0.7.8 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/hashicorp/golang-lru v0.6.0 // indirect
	github.com/hashicorp/serf v0.10.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/goidentity/v6 v6.0.1 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolio/noiseconn v0.0.0-20230301220541-88105e6c8ac6 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.5 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-runewidth v0.0.15 // indirect
	github.com/miekg/dns v1.1.61 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mozillazg/go-httpheader v0.2.1 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/oklog/ulid v1.3.1 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/pengsrc/go-shared v0.2.1-0.20190131101655-1999055a4a14 // indirect
	github.com/pingcap/errors v0.11.5-0.20211224045212-9687c2b0f87c // indirect
	github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c // indirect
	github.com/pingcap/kvproto v0.0.0-20230403051650-e166ae588106 // indirect
	github.com/pkg/browser v0.0.0-20240102092130-5ac0b6a4141c // indirect
	github.com/planetscale/vtprotobuf v0.6.1-0.20240319094008-0393e58bdf10 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/pquerna/ffjson v0.0.0-20190930134022-aa0246cd15f7 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.62.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rasky/go-xdr v0.0.0-20170124162913-1a41d1a06c93 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spacemonkeygo/monkit/v3 v3.0.25-0.20251022131615-eb24eb109368 // indirect
	github.com/spiffe/go-spiffe/v2 v2.6.0 // indirect
	github.com/syndtr/goleveldb v1.0.0 // indirect
	github.com/tiancaiamao/gp v0.0.0-20221230034425-4025bc8a4d4a // indirect
	github.com/tikv/pd/client v0.0.0-20230329114254-1948c247c2b1 // indirect
	github.com/twmb/murmur3 v1.1.8 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.67.0 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	github.com/zeebo/blake3 v0.2.3 // indirect
	github.com/zeebo/errs v1.4.0 // indirect
	go.etcd.io/etcd/api/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
	go.mongodb.org/mongo-driver v1.17.6 // indirect
	go.opencensus.io v0.24.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/detectors/gcp v1.39.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.68.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.68.0 // indirect
	go.opentelemetry.io/otel v1.43.0 // indirect
	go.opentelemetry.io/otel/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/sdk v1.43.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.43.0 // indirect
	go.opentelemetry.io/otel/trace v1.43.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go.yaml.in/yaml/v2 v2.4.3 // indirect
	golang.org/x/exp v0.0.0-20240119083558-1b970713d09a // indirect
	golang.org/x/sync v0.20.0 // indirect
	golang.org/x/text v0.35.0 // indirect
	golang.org/x/time v0.8.0 // indirect
	google.golang.org/genproto v0.0.0-20241118233622-e639e219e697 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260120221211-b8f7ae30c516 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260406210006-6f92a3bedf2d // indirect
	google.golang.org/grpc v1.80.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	modernc.org/fileutil v1.0.0 // indirect
	sigs.k8s.io/yaml v1.6.0 // indirect
	storj.io/common v0.0.0-20260514184426-9f076a4a8d52 // indirect
	storj.io/drpc v0.0.35-0.20250513201419-f7819ea69b55 // indirect
	storj.io/eventkit v0.0.0-20250410172343-61f26d3de156 // indirect
	storj.io/infectious v0.0.2 // indirect
	storj.io/picobuf v0.0.4 // indirect
	xorm.io/builder v0.3.7 // indirect
)

replace github.com/minio/minio v0.0.0-20210206053228-97fe57bba92c => github.com/juicedata/minio v0.0.0-20260515071949-69a6cfc9da65

replace github.com/hanwen/go-fuse/v2 v2.1.1-0.20210611132105-24a1dfe6b4f8 => github.com/juicedata/go-fuse/v2 v2.1.1-0.20260324060633-bc6464442863

replace github.com/dgrijalva/jwt-go v3.2.0+incompatible => github.com/golang-jwt/jwt v3.2.1+incompatible

replace github.com/vbauerster/mpb/v7 v7.0.3 => github.com/juicedata/mpb/v7 v7.0.4-0.20231024073412-2b8d31be510b

replace xorm.io/xorm v1.0.7 => gitea.com/davies/xorm v1.0.8-0.20220528043536-552d84d1b34a

replace github.com/huaweicloud/huaweicloud-sdk-go-obs v3.21.12+incompatible => github.com/juicedata/huaweicloud-sdk-go-obs v3.22.12-0.20230228031208-386e87b5c091+incompatible

replace github.com/urfave/cli/v2 v2.19.3 => github.com/juicedata/cli/v2 v2.19.4-0.20230605075551-9c9c5c0dce83

replace github.com/vmware/go-nfs-client v0.0.0-20190605212624-d43b92724c1b => github.com/juicedata/go-nfs-client v0.0.0-20250220101412-d3a8c1ca64a1

replace github.com/mattn/go-colorable v0.1.13 => github.com/juicedata/go-colorable v0.0.0-20250208072043-a97a0c2023db

replace github.com/mattn/go-colorable v0.1.12 => github.com/juicedata/go-colorable v0.0.0-20250208072043-a97a0c2023db

replace github.com/mattn/go-colorable v0.1.4 => github.com/juicedata/go-colorable v0.0.0-20250208072043-a97a0c2023db

replace github.com/mattn/go-colorable v0.1.6 => github.com/juicedata/go-colorable v0.0.0-20250208072043-a97a0c2023db

replace github.com/mattn/go-colorable v0.1.9 => github.com/juicedata/go-colorable v0.0.0-20250208072043-a97a0c2023db

replace github.com/mattn/go-colorable v0.0.9 => github.com/juicedata/go-colorable v0.0.0-20250208072043-a97a0c2023db

replace github.com/cloudsoda/go-smb2 => github.com/juicedata/go-smb2 v0.0.0-20260310064141-58f27d06634e

replace github.com/hashicorp/golang-lru/v2 v2.0.7 => github.com/juicedata/golang-lru/v2 v2.0.8-0.20251126062551-1b321869f904
