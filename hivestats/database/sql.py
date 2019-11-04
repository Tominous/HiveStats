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

    def table_exists(self, name):
        """Check if a table exists

        Args:
            name (str): name of table to checks

        Returns:
            bool: whether table exists
        """
        self.cursor.execute(
            """
                select exists(
                    select * from information_schema.tables
                        where table_name=%(name)s
                    );
            """,
            {"name": name},
        )

        return self.cursor.fetchone()[0]

    def create_table(
        self, name, columns=None, types=None, *, force=False, raise_error=True
    ):
        """Create new table

        Args:
            name (str): name to give table
            columns (Tuple[str], optional): column names
            types (Tuple[str], optional): type enforcement for columns
            force (bool, optional): this determines whether to forcibly replace delete
                                    and replace table if it already exists
                                    defaults to False
            raise_error (bool, optional): if table exists and force is false, an error
                                          is thrown; if raise_error is false, this
                                          function will fail silently
                                          defaults to True
        """
        if columns:
            if not types:
                types = [""] * len(columns)

            col_args = ", ".join(
                [f"{col_name} {col_type}" for col_name, col_type in zip(columns, types)]
            )
        else:
            col_args = ""

        if self.table_exists(name):
            if force:
                self.drop_table(name)
            elif raise_error:
                raise DuplicateTable(f"{name} already exists")
            else:
                return False

        self.cursor.execute(
            """
                create table %(name)s (%(col_args)s);
            """,
            {"name": AsIs(name), "col_args": AsIs(col_args)},
        )

        return True

    def replace_table(self, name, columns, types, values):
        """Replace existing table with new data

        Args:
            name (str): name to give table
            columns (Tuple[str], optional): column names
            types (Tuple[str], optional): type enforcement for columns
            values (Tuple(Tuple(Any))): values to be inserted into new table
        """

        temp_table = f"{name}_temp"
        old_table = f"{name}_old"

        self.create_table(temp_table, columns, types, force=True)
        self.insert(temp_table, columns, values)

        self.rename_table(name, old_table)
        self.rename_table(temp_table, name)

        self.drop_table(old_table)

    def drop_table(self, name):
        """Drop table if it exists

        Args:
            name (str): table of table to drop
        """
        self.cursor.execute(
            """
                drop table if exists %(table)s;
            """,
            {"name": AsIs(name)},
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
            conflict_key (str, optional): name of a column to resolve row conflicts on
                                          if provided, an upsert is performed and this
                                          column is used to determine and update rows

        Note:
            if provided, the column used as conflict_key must be constrained as unique
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
            raise_error (bool, optional): if constraint exists, an error is thrown; if
                                          raise_error is false, this function will fail
                                          silently
                                          defaults to True
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
