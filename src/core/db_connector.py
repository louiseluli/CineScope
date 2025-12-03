"""
CineScope Database Connector

Provides a simple interface for connecting to and querying SQLite databases
using SQLAlchemy and Pandas for robust data handling.
"""
import pandas as pd
import logging
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
from typing import Optional, List

logger = logging.getLogger(__name__)

class DBConnector:
    """Handles connections and queries to a SQLite database."""
    
    def __init__(self, db_path: str):
        """
        Initializes the connector.
        
        Args:
            db_path (str): The full path to the SQLite database file.
        """
        self.db_path = db_path
        self.engine: Optional[Engine] = None
        self._connect()

    def _connect(self):
        """Creates a SQLAlchemy engine for the database connection."""
        try:
            self.engine = create_engine(f'sqlite:///{self.db_path}')
            logger.info(f"Successfully created engine for database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to create engine for {self.db_path}: {e}")
            self.engine = None

    def get_table_names(self) -> List[str]:
        """Returns a list of table names in the database."""
        if not self.engine:
            return []
        try:
            inspector = inspect(self.engine)
            return inspector.get_table_names()
        except Exception as e:
            logger.error(f"Could not retrieve table names from {self.db_path}: {e}")
            return []

    def get_table_as_df(self, table_name: str) -> Optional[pd.DataFrame]:
        """
        Reads an entire table from the database into a pandas DataFrame.
        
        Args:
            table_name (str): The name of the table to read.
            
        Returns:
            A pandas DataFrame with the table's content, or None on error.
        """
        if not self.engine:
            logger.error("Database engine is not available.")
            return None
            
        try:
            df = pd.read_sql_table(table_name, self.engine)
            logger.info(f"Successfully loaded '{table_name}' table into DataFrame.")
            return df
        except Exception as e:
            logger.error(f"Could not read table '{table_name}' from {self.db_path}: {e}")
            return None