import os
import psycopg2
from psycopg2.extensions import AsIs, ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import DictCursor
from psycopg2.errors import DuplicateTable


class Postgres:
    """Used to setup connection to and interact with internal Postgres database
    """
    def __init__(self):
        self._conn = psycopg2.connect(
            os.environ["DATABASE_URL"],
            sslmode="require",
        )

        self._conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        self.cursor = self._conn.cursor(cursor_factory=DictCursor)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._conn.close()

    def table_exists(self, table):
        """Check if a table exists

        Args:
            table (str): name of table to checks

        Returns:
            bool: whether table exists
        """
        self.cursor.execute(
            """
                select exists(
                    select * from information_schema.tables
                        where table_name=%(table)s
                    );
            """,
            {"table": table},
        )

        return self.cursor.fetchone()[0]

    def create_table(
        self, table, columns=None, types=None, *, force=False, raise_error=True
    ):
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

        if self.table_exists(table):
            if force:
                self.drop_table(table)
            elif raise_error:
                raise DuplicateTable(f"{table} already exists")
            else:
                return False

        self.cursor.execute(
            """
                create table %(table)s (%(col_args)s);
            """,
            {"table": AsIs(table), "col_args": AsIs(col_args)},
        )

        return True

    def replace_table(self, table, columns, types, values):
        """Replace existing table with new data

        Args:
            name (str): name to give table
            columns (Tuple[str], optional): column names
            types (Tuple[str], optional): type enforcement for columns
            values (Tuple(Tuple(Any))): values to be inserted into new table
        """

        temp_table = f"{table}_temp"
        old_table = f"{table}_old"

        self.create_table(temp_table, columns, types, force=True)
        self.insert(temp_table, columns, values)

        self.rename_table(table, old_table)
        self.rename_table(temp_table, table)

        self.drop_table(old_table)

    def drop_table(self, table):
        """Drop table if it exists

        Args:
            table (str): table of table to drop
        """
        self.cursor.execute(
            """
                drop table if exists %(table)s;
            """,
            {"table": AsIs(table)},
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
                    rename to %(new_name)s;
            """,
            {"name": AsIs(name), "new_name": AsIs(new_name)},
        )

    def insert(self, table, columns, values, *, conflict_key=None):
        """Insert new values into existing table

        Args:
            table (str): name of table to insert values into
            columns (Tuple[str]): tuple of columns to insert values into
            values (Tuple(Tuple(Any))): values to be inserted into new table
        """
        conflict_clause = ""

        if conflict_key:
            mapping = ", ".join([f"{col} = excluded.{col}" for col in columns])
            conflict_clause = f"""
                on conflict ({conflict_key}) do update
                    set {mapping}"""

        self.cursor.execute(
            """
                insert into %(table)s (%(columns)s)
                    values %(values)s
                %(conflict)s;
            """,
            {
                "table": AsIs(table),
                "columns": AsIs(", ".join(columns)),
                "values": AsIs(", ".join([str(row) for row in values])),
                "conflict": AsIs(conflict_clause),
            },
        )

    def add_constraint(
        self, table, column, constraint, constraint_name=None, *, raise_error=True
    ):
        """Add constraint to a specific column in a table

        Args:
            table (str): name of table that column is in
            column (str): name of column to add constraint to
            constraint (str): the type of constraint to add
            constraint_name (str, optional): specific name for constraint, set to
                                             {table}_{column}_{constraint} by default
        """
        if not constraint_name:
            constraint_name = f"{table}_{column}_{constraint}"

        try:
            self.cursor.execute(
                """
                    alter table %(table)s
                        add constraint %(constraint_name)s %(constraint)s (%(column)s)
                """,
                {
                    "table": AsIs(table),
                    "column": AsIs(column),
                    "constraint": AsIs(constraint),
                    "constraint_name": AsIs(constraint_name),
                },
            )
        except DuplicateTable:
            if raise_error:
                raise DuplicateTable(f"Constraint {constraint_name} already exists")
