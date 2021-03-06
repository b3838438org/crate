#
# Licensed to Crate.io Inc. (Crate) under one or more contributor license
# agreements.  See the NOTICE file distributed with this work for additional
# information regarding copyright ownership.  Crate licenses this file to
# you under the Apache License, Version 2.0 (the "License");  you may not
# use this file except in compliance with the License.  You may obtain a
# copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
# License for the specific language governing permissions and limitations
# under the License.
#
# However, to use any modules in this file marked as "Enterprise Features",
# Crate must have given you permission to enable and use such Enterprise
# Features and you must have a valid Enterprise or Subscription Agreement
# with Crate.  If you enable or use the Enterprise Features, you represent
# and warrant that you have a valid Enterprise or Subscription Agreement
# with Crate.  Your use of the Enterprise Features if governed by the terms
# and conditions of your Enterprise or Subscription Agreement with Crate.

from unittest import TestCase
from testutils.paths import crate_path
from testutils.ports import bind_port
from cr8.run_crate import CrateNode
from crate.client import connect
from dnslib.server import DNSServer
from dnslib.zoneresolver import ZoneResolver


class DnsSrvDiscoveryTest(TestCase):
    
    num_nodes = 3

    def setUp(self):
        zone_file = '''
crate.internal.               600   IN   SOA   localhost localhost ( 2007120710 1d 2h 4w 1h )
crate.internal.               400   IN   NS    localhost
crate.internal.               600   IN   A     127.0.0.1'''

        transport_ports = [bind_port() for _ in range(self.num_nodes)]
        for port in transport_ports:
            zone_file += '''
_test._srv.crate.internal.    600   IN   SRV   1 10 {port} 127.0.0.1.'''.format(port=port)

        dns_port = bind_port()
        self.dns_server = DNSServer(ZoneResolver(zone_file), port=dns_port)
        self.dns_server.start_thread()

        self.nodes = nodes = []
        for i in range(self.num_nodes):
            node = CrateNode(
                crate_dir=crate_path(),
                version=(4, 0, 0),
                settings={
                    'node.name': f'node-{i}',
                    'cluster.name': 'crate-dns-discovery',
                    'psql.port': 0,
                    'transport.tcp.port': transport_ports[i],
                    "discovery.zen.hosts_provider": "srv",
                    "discovery.srv.query": "_test._srv.crate.internal.",
                    "discovery.srv.resolver": "127.0.0.1:" + str(dns_port)
                },
                env={
                    'CRATE_HEAP_SIZE': '256M'
                }
            )
            node.start()
            nodes.append(node)

    def tearDown(self):
        for node in self.nodes:
            node.stop()
        self.dns_server.server.server_close()
        self.dns_server.stop()

    def test_nodes_discover_each_other(self):
        with connect(self.nodes[0].http_url) as conn:
            c = conn.cursor()
            c.execute('''select count(*) from sys.nodes''')
            result = c.fetchone()
        self.assertEqual(result[0], self.num_nodes, 'Nodes must be able to join')
