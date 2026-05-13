import re
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LocalExecutor:
    def __init__(self, storage):
        self.storage = storage
    
    def execute_select(self, query):
        logger.info(f"Executing SELECT: {query}")
        results = self.storage.execute_query(query)
        return {
            "count": len(results),
            "rows": results,
            "columns": list(results[0].keys()) if results else []
        }
    
    def execute_aggregate(self, query):
        logger.info(f"Executing AGGREGATE: {query}")
        results = self.storage.execute_query(query)
        return results[0] if results else {}
    
    def execute_insert(self, table_name, rows):
        if not rows:
            return 0
        
        columns = list(rows[0].keys())
        placeholders = ','.join(['%s'] * len(columns))
        columns_str = ','.join(columns)
        
        total_inserted = 0
        for row in rows:
            values = [row[col] for col in columns]
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            total_inserted += self.storage.execute_insert(query, values)
        
        logger.info(f"Inserted {total_inserted} rows into {table_name}")
        return total_inserted
    
    def execute_update(self, query):
        return self.storage.execute_update(query)
    
    def execute_delete(self, query):
        return self.storage.execute_update(query)
    
    def map_word_count(self, chunk_data):
        """Map function for word count"""
        text = chunk_data if isinstance(chunk_data, str) else chunk_data.decode('utf-8')
        words = re.findall(r'\b\w+\b', text.lower())
        word_count = {}
        for word in words:
            word_count[word] = word_count.get(word, 0) + 1
        return word_count
    
    def create_table(self, db_name, table_name, schema):
        logger.info(f"Creating table {db_name}.{table_name}")
        self.storage.execute_update(f"USE {db_name}")
        return self.storage.create_table(table_name, schema)