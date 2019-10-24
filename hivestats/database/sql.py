import os
import psycopg2
from psycopg2.extensions import AsIs, ISOLATION_LEVEL_AUTOCOMMIT


class Postgres:
    """Used to setup connection to and interact with internal Postgres database
    """
    def __init__(self):
        self._conn = psycopg2.connect(
            os.environ["DATABASE_URL"],
            sslmode="require",
        )

        self._conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._conn.close()

    def create_table(self, table, columns=None, types=None):
        """Create new table

        Args:
            table (str): name to give table
            columns (Tuple[str], optional): column names
            types (Tuple[str], optional): type enforcement for columns
        """
        if columns:
            if not types:
                types = [""] * len(columns)

            col_args = ", ".join(
                [f"{col_name} {col_type}" for col_name, col_type in zip(columns, types)]
            )
        else:
            col_args = ""

        self.drop_table(table)

        self.cursor.execute(
            """
                create table %(table)s (%(col_args)s);
            """,
            {"table": AsIs(table), "col_args": AsIs(col_args)},
        )

    def replace_table(self, table, columns, types, values):
        """Replace existing table with new data

        Args:
            name (str): name to give table
            columns (Tuple[str], optional): column names
            types (Tuple[str], optional): type enforcement for columns
            values (Tuple(Tuple(Any))): values to be inserted into new table
        """

        temp_table = f"{table}_temp"

        self.create_table(temp_table, columns, types)
        self.insert(temp_table, columns, values)

        self.drop_table(table)
        self.rename_table(temp_table, table)

    def drop_table(self, name):
        """Drop table if it exists

        Args:
            name (str): name of table to drop
        """
        self.cursor.execute(
            """
                drop table if exists %(table)s;
            """,
            {"table": AsIs(name)},
        )

    def rename_table(self, name, new_name):
        """Renames an existing table

        Args:
            name (str): name of table to rename
            new_name (str): new name of table
        """
        self.cursor.execute(
            """
                alter table %(name)s
                    rename to %(new_name)s
            """,
            {"name": AsIs(name), "new_name": AsIs(new_name)},
        )

    def insert(self, table, columns, values):
        """Insert new values into existing table

        Args:
            table (str): name of table to insert values into
            columns (Tuple[str]): tuple of columns to insert values into
            values (Tuple(Tuple(Any))): values to be inserted into new table
        """
        self.cursor.execute(
            """
                insert into %(table)s (%(columns)s)
                    values %(values)s;
            """,
            {
                "table": AsIs(table),
                "columns": AsIs(", ".join(columns)),
                "values": AsIs(", ".join([str(row) for row in values])),
            },
        )
