set -x

rm -rf data log
# create bucket "tiflash-test" on minio
mkdir -pv data/minio/tiflash-test

docker compose -f cluster.yaml up
