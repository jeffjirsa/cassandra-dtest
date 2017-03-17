import os
import time

from cassandra import ConsistencyLevel

from ccmlib.node import TimeoutError
from dtest import DISABLE_VNODES, Tester, create_ks, debug
from tools.data import create_c1c2_table, insert_c1c2, query_c1c2
from tools.decorators import no_vnodes, since


@since('3.0')
class TestHintedHandoffConfig(Tester):
    """
    Tests the hinted handoff configuration options introduced in
    CASSANDRA-9035.

    @jira_ticket CASSANDRA-9035
    """

    def _start_two_node_cluster(self, config_options=None):
        """
        Start a cluster with two nodes and return them
        """
        cluster = self.cluster

        if config_options:
            cluster.set_configuration_options(values=config_options)

        if DISABLE_VNODES:
            cluster.populate([2]).start()
        else:
            tokens = cluster.balanced_tokens(2)
            cluster.populate([2], tokens=tokens).start()

        return cluster.nodelist()

    def _launch_nodetool_cmd(self, node, cmd):
        """
        Launch a nodetool command and check there is no error, return the result
        """
        out, err, _ = node.nodetool(cmd)
        self.assertEqual('', err)
        return out

    def _do_hinted_handoff(self, node1, node2, enabled):
        """
        Test that if we stop one node the other one
        will store hints only when hinted handoff is enabled
        """
        session = self.patient_exclusive_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)

        node2.stop(wait_other_notice=True)

        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)

        log_mark = node1.mark_log()
        node2.start(wait_other_notice=True)

        if enabled:
            node1.watch_log_for(["Finished hinted"], from_mark=log_mark, timeout=120)

        node1.stop(wait_other_notice=True)

        # Check node2 for all the keys that should have been delivered via HH if enabled or not if not enabled
        session = self.patient_exclusive_cql_connection(node2, keyspace='ks')
        for n in xrange(0, 100):
            if enabled:
                query_c1c2(session, n, ConsistencyLevel.ONE)
            else:
                query_c1c2(session, n, ConsistencyLevel.ONE, tolerate_missing=True, must_be_missing=True)

    def nodetool_test(self):
        """
        Test various nodetool commands
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

            self._launch_nodetool_cmd(node, 'disablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is not running', res.rstrip())

            self._launch_nodetool_cmd(node, 'enablehandoff')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

            self._launch_nodetool_cmd(node, 'disablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep), res.rstrip())

            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

    def hintedhandoff_disabled_test(self):
        """
        Test gloabl hinted handoff disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': False})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is not running', res.rstrip())

        self._do_hinted_handoff(node1, node2, False)

    def hintedhandoff_enabled_test(self):
        """
        Test global hinted handoff enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

        self._do_hinted_handoff(node1, node2, True)

    def hintedhandoff_dc_disabled_test(self):
        """
        Test global hinted handoff enabled with the dc disabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True,
                                                     'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep), res.rstrip())

        self._do_hinted_handoff(node1, node2, False)

    def hintedhandoff_dc_reenabled_test(self):
        """
        Test global hinted handoff enabled with the dc disabled first and then re-enabled
        """
        node1, node2 = self._start_two_node_cluster({'hinted_handoff_enabled': True,
                                                     'hinted_handoff_disabled_datacenters': ['dc1']})

        for node in node1, node2:
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running{}Data center dc1 is disabled'.format(os.linesep), res.rstrip())

        for node in node1, node2:
            self._launch_nodetool_cmd(node, 'enablehintsfordc dc1')
            res = self._launch_nodetool_cmd(node, 'statushandoff')
            self.assertEqual('Hinted handoff is running', res.rstrip())

        self._do_hinted_handoff(node1, node2, True)


class TestHintedHandoff(Tester):

    @no_vnodes()
    def hintedhandoff_decom_test(self):
        self.cluster.populate(4).start(wait_for_binary_proto=True)
        [node1, node2, node3, node4] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)
        node4.stop(wait_other_notice=True)
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.ONE)
        node1.decommission()
        node4.start(wait_for_binary_proto=True)

        force = True if self.cluster.version() >= '3.12' else False
        node2.decommission(force=force)
        node3.decommission(force=force)

        time.sleep(5)
        for x in xrange(0, 100):
            query_c1c2(session, x, ConsistencyLevel.ONE)

    @no_vnodes()
    def hintedhandoff_delivery_to_decom_test(self):
        """
        @jira_ticket CASSANDRA-13308

        Hint delivery in progress during decom can block gossip
        """

        self.cluster.populate(4, install_byteman=True).start(wait_for_binary_proto=True)
        [node1, node2, node3, node4] = self.cluster.nodelist()
        session = self.patient_cql_connection(node1)
        create_ks(session, 'ks', 2)
        create_c1c2_table(self, session)

        # Insert data so there's something to decom
        insert_c1c2(session, n=100, consistency=ConsistencyLevel.QUORUM)

        node4.stop(wait_other_notice=True)

        # Stop hint delivery, because we'll need to get the node started and byteman setup before we start delivering hints
        for node in node1, node2, node3:
            node.nodetool("pausehandoff")

        for node in node1, node2, node3:
            node.byteman_submit(['./byteman/hint_never_complete.btm'])

        insert_c1c2(session, n=1000, consistency=ConsistencyLevel.ONE)
        # We start the node, and then immediately decommission it as soon as hint delivery starts
        node4.start(wait_for_binary_proto=True)

        # We can resume hint delivery, which shouldn't complete because of the byteman rules on 1-3
        for node in node1, node2, node3:
            node.nodetool("resumehandoff")

        mark1 = node1.mark_log()
        mark2 = node2.mark_log()
        mark3 = node3.mark_log()
        mark4 = node4.mark_log()

        node4.decommission()
        node1.watch_log_for("Removing tokens", timeout=60, from_mark=mark1)
        node4.watch_log_for("DECOMMISSIONED", timeout=60, from_mark=mark4)

        # Now make sure gossip isnt blocked, by checking for it in each log
        try:
            debug("do we see blocked gossip in node1?")
            node1.watch_log_for("pending tasks; skipping status check", timeout=10, from_mark=mark1)
            assert(False)
        except TimeoutError as e:
            # Expected, continue
            pass

        try:
            debug("do we see blocked gossip in node2?")
            node2.watch_log_for("pending tasks; skipping status check", timeout=10, from_mark=mark2)
            assert(False)
        except TimeoutError as e:
            # Expected, continue
            pass

        try:
            debug("do we see blocked gossip in node3?")
            node3.watch_log_for("pending tasks; skipping status check", timeout=10, from_mark=mark3)
            assert(False)
        except TimeoutError as e:
            # Expected, continue
            pass



