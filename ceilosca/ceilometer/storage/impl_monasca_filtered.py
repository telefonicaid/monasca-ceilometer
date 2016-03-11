#
# Copyright 2016 Telefónica Investigación y Desarrollo, S.A.U.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.


from oslo_log import log
from ceilometer.i18n import _
from ceilometer.storage.impl_monasca import Connection as MonascaBaseConnection
from ceilometer.publisher.monasca_metric_filter import MonascaMetricFilter

LOG = log.getLogger(__name__)


class Connection(MonascaBaseConnection):
    def __init__(self, url):
        super(Connection, self).__init__(url)
        self.mon_filter = MonascaMetricFilter()

    def record_metering_data(self, data):
        """Write the data to the backend storage system.

        :param data: a dictionary such as returned by
                     ceilometer.meter.meter_message_from_counter.
        """
        metric = self.mon_filter.process_sample_for_monasca(data)
        if metric:
            LOG.info(_('metering data %(counter_name)s for %(resource_id)s: '
                       '%(counter_volume)s')
                     % ({'counter_name': data['counter_name'],
                         'resource_id': data['resource_id'],
                         'counter_volume': data['counter_volume']}))
            self.mc.metrics_create(**metric)
