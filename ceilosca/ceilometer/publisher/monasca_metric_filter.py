#
# Copyright 2016 Telefonica Investigacion y Desarrollo, S.A.U.
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
from ceilometer.i18n import _LI
from ceilometer.publisher.monasca_data_filter import MonascaDataFilter

LOG = log.getLogger(__name__)


class MonascaMetricFilter(MonascaDataFilter):
    """A specialization of MonascaDataFilter which allows filtering the metrics to be stored.

    If the configuration file `monasca_field_definitions.yaml` includes a key `metrics`, then only the metrics whose
    name is listed in that key will be stored; otherwise, metrics list is taken from `metadata` keys.
    """

    def __init__(self):
        super(MonascaMetricFilter, self).__init__()
        self._metrics = self._mapping.get('metrics', None) or self._mapping['metadata'].keys()

    def process_sample_for_monasca(self, sample_obj):
        """Returns a new metric built from the given sample, or `None` if discarded."""
        metric = super(MonascaMetricFilter, self).process_sample_for_monasca(sample_obj)
        if metric['name'] not in self._metrics:
            LOG.debug(_LI("Discarded metric with name %s"), metric['name'])
            metric = None

        return metric
