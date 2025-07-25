import os
from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Type, TypeVar, cast


import pandas as pd
import pyodbc
import pytds
from pytds.tds_types import TableValuedParam


from sqlalchemy import create_engine, delete, Table, func, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase
from ir_modernized.models.entity import job_id_seq
from config import (
    SERVER as DEFAULT_SERVER,
    IR_DATABASE as DEFAULT_DATABASE,
    ODS_DATABASE as ODS_DEFAULT_DATABASE,
)
from ir_modernized.models.base import Base


# from ir_modernized.models import Person, PersonToken, OrganizationToken




class AbstractUnitTestManager(ABC):
    def __init__(
        self,
        database: "Database",
        base: Optional[DeclarativeBase] = None,
        tables: Optional[List[Type[DeclarativeBase]]] = None,
    ) -> None:
        """
        Initialize the AbstractDatabaseManager.


        :param database: The database instance (e.g., SqliteDatabase or SqlServerDatabase).
        :param base: The declarative base containing the ORM models. Optional.
        :param tables: A list of specific tables to manage. Optional.
        """
        if base is None and tables is None:
            raise ValueError("Either 'base' or 'tables' must be provided.")


        self.database = database
        self.base = base
        self.tables = tables


    @abstractmethod
    def setup_database(self) -> "Database":
        """Set up the database by creating all tables."""
        pass


    @abstractmethod
    def teardown_database(self) -> None:
        """Tear down the database by dropping all tables."""
        pass




class SqlServerUnitTestManager(AbstractUnitTestManager):
    def setup_database(self) -> "Database":
        """
        Set up the SQL Server database by truncating all specified tables.
        """
        if self.tables is None:
            raise ValueError("The 'tables' parameter must be provided for SQL Server.")


        self.database.connect()
        with self.database.get_database_session() as session:
            self._truncate_tables(session, self.tables)
            print("Tables truncated during setup.")
        return self.database


    def teardown_database(self) -> None:
        """
        Tear down the SQL Server database by truncating all specified tables.
        """
        if self.tables is None:
            raise ValueError("The 'tables' parameter must be provided for SQL Server.")


        with self.database.get_database_session() as session:
            self._truncate_tables(session, self.tables)
            print("Tables truncated during teardown.")


        self.database.disconnect()


    def _truncate_tables(
        self, session: Session, tables: List[Type[DeclarativeBase]]
    ) -> None:
        """
        Truncate specific tables in the database.


        :param session: The database session.
        :param tables: A list of ORM model classes representing the tables to truncate.
        """
        for model in tables:
            session.execute(delete(model))
        session.commit()




class SqliteUnitTestManager(AbstractUnitTestManager):
    def setup_database(self) -> "Database":
        """
        Set up the SQLite database by creating tables.
        """
        self.database.connect()
        assert self.database.engine is not None


        if self.tables is not None:
            for model in self.tables:
                table = cast(Table, model.__table__)
                table.create(self.database.engine, checkfirst=True)
                print(f"Table {table.name} created.")
        elif self.base is not None:
            self.base.metadata.create_all(self.database.engine)
            print("All tables from base created.")
        return self.database


    def teardown_database(self) -> None:
        """
        Tear down the SQLite database by dropping tables.
        """
        if self.database.engine:
            if self.tables is not None:
                for model in self.tables:
                    table = cast(Table, model.__table__)
                    table.drop(self.database.engine, checkfirst=True)
                    print(f"Table {table.name} dropped.")
            elif self.base is not None:
                self.base.metadata.drop_all(self.database.engine)
                print("All tables from base dropped.")


            self.database.disconnect()
            print("Database teardown complete.")




class Database(ABC):
    """Abstract base class for database implementations."""


    def __init__(self, database: str | None = None) -> None:
        """
        Initialize the database with the specified database name.


        :param database: The name of the database to connect to.
        """
        self._database = database
        self.connection_string = self._build_connection_string()
        self.engine = None
        self._manager: Optional[AbstractUnitTestManager] = (
            None  # Placeholder for UnitTestManager instance
        )


    @abstractmethod
    def _build_connection_string(self) -> str:
        """Build the connection string for the database."""
        pass


    def connect(self) -> "Database":
        """Establish a connection to the database."""
        if self.engine is None:
            print(f"Connecting to database: {self.connection_string}")
            self.engine = create_engine(self.connection_string, echo=True, future=True)
        return self


    def disconnect(self) -> None:
        """Close the database connection."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            print("Database connection closed.")


    def get_database_session(self):
        """Provides a new database session."""
        if self.engine is None:
            self.connect()
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
        return SessionLocal()


    @abstractmethod
    def database(self) -> str:
        """Return the database name."""
        pass


    @abstractmethod
    def call_stored_procedure_with_tvp(
        self, procedure_name: str, tvp_param: TableValuedParam
    ) -> pd.DataFrame:
        """
        Call a stored procedure with a Table-Valued Parameter (TVP).
        Subclasses must implement this method.
        """
        pass


    @abstractmethod
    def setup_database(
        self,
        base: Optional[DeclarativeBase] = None,
        tables: Optional[List[Type[DeclarativeBase]]] = None,
    ) -> "Database":
        """
        Set up the database by creating all tables.
        Subclasses must implement this method.
        """
        pass


    def teardown_database(self) -> None:
        """
        Tear down the database by delegating to the UnitTestManager.
        """
        if self._manager is None:
            raise RuntimeError(
                "UnitTestManager is not initialized. Initialize it before calling teardown_database()."
            )


        self._manager.teardown_database()


    @abstractmethod
    def get_job_id(self) -> int:
        """
        Get the next unique JobID for logging purposes.


        Returns:
            int: The next unique JobID.
        """
        pass




class SqlServerDatabase(Database):
    """Concrete implementation for SQL Server."""


    DRIVER = "ODBC Driver 17 for SQL Server"


    def __init__(self, database: str = DEFAULT_DATABASE) -> None:
        """
        Initialize the SqlServerDatabase with the specified database.


        :param database: The name of the database to connect to. Defaults to DEFAULT_DATABASE.
        """
        self._database = database
        self.engine: Optional[Engine] = None
        self._server = DEFAULT_SERVER
        super().__init__(database)


        # Set up pytds connection information
        self.pytds_connection_info = {
            "dsn": self._server,
            "database": self._database,
            "auth": pytds.login.SspiAuth(),  # Use Windows Authentication
        }


    def _build_connection_string(self) -> str:
        """Build the connection string for SQL Server."""
        server = os.environ.get("SERVER", DEFAULT_SERVER)
        return f"mssql+pyodbc://@{server}/{self.database()}?driver={self.DRIVER}"


    def database(self) -> str:
        """Return the database name."""
        return self._database


    def call_stored_procedure_with_tvp(
        self, procedure_name: str, tvp_param: TableValuedParam
    ) -> pd.DataFrame:
        """
        Call a stored procedure with a Table-Valued Parameter (TVP) using pytds
        and return the result as a pandas DataFrame.


        :param procedure_name: The name of the stored procedure to call.
        :param tvp_param: A TableValuedParam object representing the TVP.
        :return: A pandas DataFrame containing the result of the stored procedure.
        """
        # Use pytds for TVP-specific operations
        with pytds.connect(**self.pytds_connection_info) as conn:
            cursor = conn.cursor()


            # Construct the EXEC SQL statement
            sql = f"EXEC {procedure_name} @tvp = %s"


            # Execute the stored procedure with the TVP
            cursor.execute(sql, (tvp_param,))


            # Fetch the results
            result = cursor.fetchall()


            # Cast cursor.description to the expected type. Otherwise, Pylance protests
            description = cast(Optional[List[Tuple[str, ...]]], cursor.description)


            # Check if description is None
            if description is None:
                # No result set was returned
                return pd.DataFrame()


            # Extract column names from the description
            column_names = [desc[0] for desc in description]


            # Convert the result to a pandas DataFrame
            dataframe = pd.DataFrame(result, columns=column_names)


        return dataframe


    def setup_database(
        self,
        base: Optional[DeclarativeBase] = None,
        tables: Optional[List[Type[DeclarativeBase]]] = None,
    ) -> Database:
        """
        Set up the SQL Server database by truncating all specified tables.
        """
        self._manager = SqlServerUnitTestManager(
            database=self, base=base, tables=tables
        )
        self._manager.setup_database()
        return self


    def get_job_id(self) -> int:
        """
        Get the next JobID using the SQL Server sequence.


        Returns:
            int: The next unique JobID.
        """
        with self.get_database_session() as session:
            result = session.execute(job_id_seq)
            job_id = result if isinstance(result, int) else result.scalar()
            if job_id is None:
                raise ValueError("Failed to retrieve a valid JobID from the sequence.")
            return job_id




class SqliteDatabase(Database):
    """Concrete implementation for SQLite."""


    def __init__(self) -> None:
        """Initialize the SqliteDatabase."""
        super().__init__(database="sqlite:///:memory:")
        self._manager: Optional[AbstractUnitTestManager] = (
            None  # Placeholder for DatabaseManager instance
        )


        # self.engine = create_engine(self.database())


        #     cursor.close()


    def _build_connection_string(self) -> str:
        """Build the connection string for SQLite."""
        return self.database()


    def connect(self) -> "Database":
        super().connect()


        # Enable SQLite foreign key enforcement
        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragma(dbapi_connection, connection_record):
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")


        return self


    def database(self) -> str:
        """Return the database name."""
        return "sqlite:///:memory:"


    def call_stored_procedure_with_tvp(
        self, procedure_name: str, tvp_param: TableValuedParam
    ) -> pd.DataFrame:
        """
        SQLite does not support stored procedures with TVPs.
        """
        raise NotImplementedError(
            "SQLite does not support stored procedures with TVPs."
        )


    def setup_database(
        self,
        base: Optional[DeclarativeBase] = None,
        tables: Optional[List[Type[DeclarativeBase]]] = None,
    ) -> Database:
        """
        Set up the SQLite database by creating all tables.


        :param base: The declarative base containing the ORM models. Optional.
        :param tables: A list of specific tables to create. Optional.
        """
        # Initialize the SqliteDatabaseManager and store it in the _manager attribute
        self._manager = SqliteUnitTestManager(database=self, base=base, tables=tables)
        self._manager.setup_database()
        with self.get_database_session() as session:
            session.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS job_id_table (
                        JobID INTEGER PRIMARY KEY AUTOINCREMENT
                    )
                """
                )
            )
            session.commit()
        return self


    def get_job_id(self) -> int:
        """
        Get the next JobID by creating and using a table with an identity column in SQLite.


        Returns:
            int: The next unique JobID.
        """
        with self.get_database_session() as session:


            # Insert a row to generate a new JobID and retrieve it
            session.execute(text("INSERT INTO job_id_table DEFAULT VALUES"))
            job_id = session.execute(text("SELECT last_insert_rowid()")).scalar()
            session.commit()
            if job_id is None:
                raise ValueError("Failed to retrieve a valid JobID from the sequence.")
            return job_id




def main():
    # This will create an in-memory SQLite database
    sqlite_db = SqliteDatabase()


    sqlite_db.connect()
    try:
        job_id_sqlite = sqlite_db.get_job_id()
        print(f"Generated SQLite JobID: {job_id_sqlite}")
        # Test a second call to verify increment
        job_id_sqlite_next = sqlite_db.get_job_id()
        print(f"Generated SQLite JobID (next): {job_id_sqlite_next}")
    finally:
        sqlite_db.disconnect()




if __name__ == "__main__":
    main()





