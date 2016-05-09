"""Convenience Validations for working with Kafka"""

from fabric.operations import run

from alarmageddon.validations.validation import Priority
from alarmageddon.validations.ssh import SshValidation

import re
from collections import Counter

import logging
import pdb

logger = logging.getLogger(__name__)

class KafkaStatusValidation(SshValidation):

    """Validate that the Kafka cluster has all of it's partitions
    distributed across the cluster.

    :param ssh_contex: An SshContext class, for accessing the hosts.

    :param zookeeper_nodes: Kafka zookeeper hosts and ports in CSV.
      e.g. "host1:2181,host2:2181,host3:2181"

    :param kafka_list_topic_command: Kafka command to list topics
      (defaults to "/opt/kafka/bin/kafka-list-topic.sh")

    :param priority: The Priority level of this validation.

    :param timeout: How long to attempt to connect to the host.

    :param hosts: The hosts to connect to.

    """

    def __init__(self, ssh_context,
                 zookeeper_nodes,
                 kafka_list_topic_command="/opt/kafka/bin/kafka-list-topic.sh",
                 priority=Priority.NORMAL, timeout=None,
                 hosts=None):
        SshValidation.__init__(self, ssh_context,
                               "Kafka partition status",
                               priority=priority,
                               timeout=timeout,
                               hosts=hosts)
        self.kafka_list_topic_command = kafka_list_topic_command
        self.zookeeper_nodes = zookeeper_nodes

    def perform_on_host(self, host):
        """Runs kafka list topic command on host"""
        output = run(
            self.kafka_list_topic_command +
            " --zookeeper " +
            self.zookeeper_nodes)

        error_patterns = [
            'No such file', 'Missing required argument', 'Exception']
        if any(x in output for x in error_patterns):
            self.fail_on_host(host, "An exception occurred while " +
                              "checking Kafka cluster health on {0} ({1})"
                              .format((host, output)))
        parsed = re.split(r'\t|\n', output)
        topics = [parsed[i] for i in xrange(0, len(parsed), 5)]
        leaders = [parsed[i] for i in xrange(2, len(parsed), 5)]

        tuples = zip(topics, leaders)
        duplicates = [x for x, y in Counter(tuples).items() if y > 1]

        if len(duplicates) != 0:
            self.fail_on_host(host, "Kafka partitions are out of sync. " +
                              "Multiple leaders for the same partition " +
                              "for the same replica: " +
                              (", ".join("%s has %s" % (
                                  dup[0], dup[1])
                                         for dup in duplicates)))


class KafkaConsumerLagMonitor(SshValidation):

    """Validate that the Kafka consumers are not lagging behind publication
    too much.

    :param ssh_contex: An SshContext class, for accessing the hosts.

    :param zookeeper_nodes: Kafka zookeeper hosts and ports in CSV.
      e.g. "host1:2181,host2:2181,host3:2181"

    :param priority: The Priority level of this validation.

    :param timeout: How long to attempt to connect to the host.

    :param hosts: The hosts to connect to.

    """

    def __init__(self, ssh_context,
                 zookeeper_nodes,
                 priority=Priority.NORMAL, timeout=None,
                 hosts=None):
        SshValidation.__init__(self, ssh_context,
                               "Kafka Consumer Lag Monitor",
                               priority=priority,
                               timeout=timeout,
                               hosts=hosts)
        self.zookeeper_nodes = zookeeper_nodes

    def perform_on_host(self, host):
        """Runs kafka ConsumerOffsetChecker"""
        output = run(
            "/usr/local/kafka/bin/kafka-run-class.sh kafka.tools.ConsumerOffsetChecker --group find_delivery" +
            " --zkconnect " +
            self.zookeeper_nodes)

        error_patterns = [
            'No such file', 'Missing required argument', 'Exception']
        if any(x in output for x in error_patterns):
            self.fail_on_host(host, "An exception occurred while " +
                              "checking Kafka consumer lag on {0} ({1})"
                              .format((host, output)))

        parsed = re.split(r'\t|\r\n', output)
        parsed = [re.split(r'\s\s+', p) for p in parsed if len(re.split(r'\s\s+', p)) > 2]
        topics = [item[1] for item in parsed if item[1] != 'Topic']
        pids = [item[2] for item in parsed if item[2].isdigit()]
        lags = [int(item[5]) for item in parsed if item[5].isdigit()]

        tuples = zip(topics, pids, lags)
        laggers = [(x, y, z) for x, y, z in tuples if z > 5]
        if len(laggers) != 0:
            self.fail_on_host(host, "Kafka consumers are lagging on topic " +
                              (", ".join("%s on PID %s is lagging by %s messages." % (
                                  lagger[0], lagger[1], lagger[2])
                                         for lagger in laggers)))
