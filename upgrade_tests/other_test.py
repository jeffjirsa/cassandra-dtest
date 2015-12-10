"""
Home for upgrade-related tests that don't fit in with the core upgrade testing in dtest.upgrade_through_versions
"""
from collections import namedtuple
from cassandra import ConsistencyLevel as CL
from datahelp import create_rows
from upgrade_base import UpgradeTester
from tools import since


class TestForRegressions(UpgradeTester):
    """
    Catch-all class for regression tests on specific versions.
    """
    NODES, RF, __test__, CL = 2, 1, True, CL.ONE

    def test_10822(self):
        session = self.prepare()

        session.execute("CREATE KEYSPACE financial WITH replication={'class':'SimpleStrategy', 'replication_factor': 1};")
        session.execute("""
        create table if not exists financial.symbol_history (
          symbol text,
          name text,
          year int,
          month int,
          day int,
          volume bigint,
          close double,
          open double,
          low double,
          high double,
          primary key((symbol, year), month, day)
        ) with CLUSTERING ORDER BY (month desc, day desc);
        """)

        data = """
            |symbol |name      |year  |month |day |volume |
            | CORP  | MegaCorp | 2004 | 1    | 1  | 100   |
            | CORP  | MegaCorp | 2004 | 2    | 1  | 100   |
            | CORP  | MegaCorp | 2004 | 3    | 1  | 100   |
            | CORP  | MegaCorp | 2004 | 4    | 1  | 100   |
            | CORP  | MegaCorp | 2004 | 5    | 1  | 100   |
        """
        create_rows(data, session, 'financial.symbol_history', cl=CL.ALL,
                    format_funcs={'symbol': unicode, 'name': unicode, 'year': int, 'month': int, 'day': int, 'volume': int})

        session.execute("delete from financial.symbol_history where symbol='CORP' and year = 2004 and month=3;")
        sessions = self.do_upgrade(session)

        for s in sessions:
            count = s[1].execute("select count(*) from financial.symbol_history where symbol='CORP' and year=2004 LIMIT 12;")[0][0]

            expected_rows = 4
            self.assertEqual(count, expected_rows, "actual {} did not match expected {}".format(count, expected_rows))
