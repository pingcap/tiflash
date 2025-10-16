# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DOCKER ?= docker
TIKV_IMAGE ?= us-docker.pkg.dev/pingcap-testing-account/hub/tikv/tikv/image:dedicated-next-gen
PD_IMAGE ?= us-docker.pkg.dev/pingcap-testing-account/hub/tikv/pd/image:master-next-gen
TIDB_IMAGE ?= us-docker.pkg.dev/pingcap-testing-account/hub/pingcap/tidb/images/tidb-server:master-next-gen
TIFLASH_IMAGE ?= us-docker.pkg.dev/pingcap-testing-account/hub/pingcap/tiflash/image:master-next-gen

# Set PULL to 1 to always pull the latest image
PULL ?= 0

pull_if_needed = if [ "$(PULL)" = "1" ] || [ "$(PULL)" = "true" ]; then \
		echo "Pulling image $(1)"; \
		$(DOCKER) pull $(1); \
	fi

tikv:
	mkdir -pv ./binaries/tikv
	$(call pull_if_needed,$(TIKV_IMAGE))
	$(DOCKER) run --entrypoint '/bin/sh' -it -v ./binaries/tikv:/data/out --rm $(TIKV_IMAGE) -c 'cp -v /tikv-server /tikv-worker /cse-ctl /data/out/'

pd:
	mkdir -pv binaries/pd
	$(call pull_if_needed,$(PD_IMAGE))
	$(DOCKER) run --entrypoint '/bin/sh' -it -v ./binaries/pd:/data/out --rm $(PD_IMAGE) -c 'cp -v /pd-server /pd-ctl /pd-recover /data/out/'

tidb:
	mkdir -pv binaries/tidb
	$(call pull_if_needed,$(TIDB_IMAGE))
	$(DOCKER) run --entrypoint '/bin/sh' -it -v ./binaries/tidb:/data/out --rm $(TIDB_IMAGE) -c 'cp -v /tidb-server /data/out/'

tiflash:
	mkdir -pv binaries/tiflash
	$(call pull_if_needed,$(TIFLASH_IMAGE))
	$(DOCKER) run --entrypoint '/bin/sh' -it -v ./binaries/tiflash:/data/out --rm $(TIFLASH_IMAGE) -c 'cp -rv /tiflash/* /data/out/'

versions:
	@echo "TiKV version:"
	./binaries/tikv/tikv-server -V || echo "not found"
	@echo "PD version:"
	./binaries/pd/pd-server -V || echo "not found"
	@echo "TiDB version:"
	./binaries/tidb/tidb-server -V || echo "not found"
	@echo "TiFlash version:"
	./binaries/tiflash/tiflash version || echo "not found"

download: tikv pd tidb tiflash versions
	
pre_package:
	mkdir -p ./binaries/package
package_tikv: pre_package
	tar -czvf ./binaries/package/tikv.tar.gz -C ./binaries/tikv .
package_pd: pre_package
	tar -czvf ./binaries/package/pd.tar.gz -C ./binaries/pd .
package_tidb: pre_package
	tar -czvf ./binaries/package/tidb.tar.gz -C ./binaries/tidb .
package_tiflash: pre_package
	tar -czvf ./binaries/package/tiflash.tar.gz -C ./binaries/ tiflash/

package: package_pd package_tidb package_tikv package_tiflash
