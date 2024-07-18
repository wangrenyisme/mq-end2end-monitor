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


FROM anapsix/alpine-java

MAINTAINER 1544476096@qq.com

WORKDIR /opt/kafka-monitor
ADD build/ build/
ADD tools/ tools/
ADD bin/xinfra-monitor-start.sh bin/xinfra-monitor-start.sh
ADD bin/kmf-run-class.sh bin/kmf-run-class.sh
ADD config/xinfra-monitor-beijing-v1.properties config/xinfra-monitor.properties
ADD config/log4j2.properties config/log4j2.properties
ADD docker/kafka-monitor-docker-entry.sh kafka-monitor-docker-entry.sh
ADD webapp/ webapp/

CMD ["/opt/kafka-monitor/kafka-monitor-docker-entry.sh"]
