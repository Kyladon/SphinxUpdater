#!/usr/bin/env python3
#
# Update Sphinx RT index when an event is triggered in MySQL binlog
# Modified from original API-based version to connect directly to Sphinx
#

import asyncio
import aiomysql
import logging
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Main PreDB Mysql connection settings
MYSQL_SETTINGS = {
    "host": "<host_here>",
    "port": <port_here>,
    "user": "<user_here>",
    "passwd": "<password_here>"
}

# Sphinx RT index connection settings
SPHINX_SETTINGS = {
    "host": "<host_here>",
    "port": <port_here>,
    "user": "<user_here>",
    "passwd": "<password_here>"
}

# Ignore and remove these unless you use group id's
# in your releases table
groups = {}
group_max_id_known = 0
sections = {}
section_max_id_known = 0

# Skip releases with IDs lower than this (for initial setup)
#last_known_insert = 14125440
last_known_insert = 14130526

# Sphinx connection pool
sphinx_pool = None

async def init_sphinx_pool():
    """Initialize the Sphinx connection pool"""
    global sphinx_pool
    sphinx_pool = await aiomysql.create_pool(
        host=SPHINX_SETTINGS['host'],
        port=SPHINX_SETTINGS['port'],
        user=SPHINX_SETTINGS['user'],
        password=SPHINX_SETTINGS['passwd'],
        autocommit=True
    )
    logger.info("Sphinx connection pool initialized")

async def execute_sphinx_command(query, params=None):
    """Execute a command on Sphinx RT index"""
    global sphinx_pool
    try:
        async with sphinx_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, params)
                return True
    except Exception as e:
        logger.error(f"Sphinx command failed: {query} - Error: {e}")
        return False

async def add_release_to_sphinx(release_data):
    """Add or update a release in Sphinx RT index"""
    replace_query = """
        REPLACE INTO releases_rt VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        release_data['id'],
        release_data.get('releasename', ''),
        release_data.get('releasename', ''),
        release_data.get('groupid') or 0,
        release_data.get('sectionid') or 0,
        release_data.get('status') or 0,
        release_data.get('pretime') or 0,
        release_data.get('size') or 0.0,
        release_data.get('files') or 0
    )
    
    success = await execute_sphinx_command(replace_query, params)
    if success:
        logger.info(f"Replaced release: {release_data['id']} - {release_data['releasename']}")
    return success

async def remove_release_from_sphinx(release_id):
    """Remove a release from Sphinx RT index"""
    delete_query = "DELETE FROM releases_rt WHERE id = %s"
    success = await execute_sphinx_command(delete_query, (release_id,))
    if success:
        logger.info(f"Deleted release: {release_id}")
    return success

async def main():
    """Main binlog monitoring loop"""
    logger.info("Starting binlog stream reader...")
    
    stream = BinLogStreamReader(
        connection_settings=MYSQL_SETTINGS,
        server_id=2,
        only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
        blocking=True
    )

    for binlogevent in stream:
        # Only process releases table events
        if binlogevent.table != "releases":
            continue

        for row in binlogevent.rows:
            try:
                if isinstance(binlogevent, DeleteRowsEvent):
                    release_id = row["values"]['id']
                    await remove_release_from_sphinx(release_id)

                elif isinstance(binlogevent, WriteRowsEvent):
                    # New release added
                    values = row['values']
                    
                    # Skip if ID is too low (for initial setup)
                    if values['id'] < last_known_insert:
                        continue
                    
                    # Only index releases that are not deleted (status != 4)
                    if values.get('status', 0) == 4:
                        continue
                    
                    release_data = {
                        'id': values['id'],
                        'releasename': values['releasename'],
                        'groupid': values.get('groupid', 0),
                        'sectionid': values.get('sectionid', 0),
                        'status': values.get('status', 0),
                        'pretime': values.get('pretime', 0),
                        'files': values.get('files', 0),
                        'size': values.get('size', 0.0)
                    }
                    
                    await add_release_to_sphinx(release_data)

                elif isinstance(binlogevent, UpdateRowsEvent):
                    # Release updated
                    before_values = row['before_values']
                    after_values = row['after_values']
                    
                    # If status changed to deleted (4), remove from index
                    if after_values.get('status') == 4 and before_values.get('status') != 4:
                        await remove_release_from_sphinx(after_values['id'])
                    
                    # If status changed from deleted to active, add to index
                    elif before_values.get('status') == 4 and after_values.get('status') != 4:
                        release_data = {
                            'id': after_values['id'],
                            'releasename': after_values['releasename'],
                            'groupid': after_values.get('groupid', 0),
                            'sectionid': after_values.get('sectionid', 0),
                            'status': after_values.get('status', 0),
                            'pretime': after_values.get('pretime', 0),
                            'files': after_values.get('files', 0),
                            'size': after_values.get('size', 0.0)
                        }
                        await add_release_to_sphinx(release_data)
                    
                    # If record is active and fields changed, update index
                    elif after_values.get('status', 0) != 4:
                        release_data = {
                            'id': after_values['id'],
                            'releasename': after_values['releasename'],
                            'groupid': after_values.get('groupid', 0),
                            'sectionid': after_values.get('sectionid', 0),
                            'status': after_values.get('status', 0),
                            'pretime': after_values.get('pretime', 0),
                            'files': after_values.get('files', 0),
                            'size': after_values.get('size', 0.0)
                        }
                        await add_release_to_sphinx(release_data)

            except Exception as e:
                logger.error(f"Error processing binlog event: {e}")
                logger.error(f"Event: {binlogevent}")
                logger.error(f"Row: {row}")
                continue

    stream.close()

async def fetch_all_groups():
    """Fetch all groups from database and cache them"""
    global groups, group_max_id_known
    
    logger.info("Fetching all known groups...")
    pool = await aiomysql.create_pool(
        host=MYSQL_SETTINGS['host'],
        port=MYSQL_SETTINGS['port'],
        user=MYSQL_SETTINGS['user'],
        password=MYSQL_SETTINGS['passwd'],
        db='predb'
    )
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT groupid, groupname FROM `groups`")
            rows = await cur.fetchall()
            
            if len(rows) < 1000:
                logger.warning("Failed to fetch groups from database!")
                return
            
            groups = dict(rows)
            group_max_id_known = max(groups.keys()) if groups else 0
            logger.info(f"Cached {len(groups)} groups. Max ID: {group_max_id_known}")
    
    pool.close()
    await pool.wait_closed()

async def fetch_all_sections():
    """Fetch all sections from database and cache them"""
    global sections, section_max_id_known
    
    logger.info("Fetching all known sections...")
    pool = await aiomysql.create_pool(
        host=MYSQL_SETTINGS['host'],
        port=MYSQL_SETTINGS['port'],
        user=MYSQL_SETTINGS['user'],
        password=MYSQL_SETTINGS['passwd'],
        db='predb'
    )
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("SELECT sectionid, sectionname FROM sections")
            rows = await cur.fetchall()
            
            if len(rows) < 40:
                logger.warning("Failed to fetch sections from database!")
                return
            
            sections = dict(rows)
            section_max_id_known = max(sections.keys()) if sections else 0
            logger.info(f"Cached {len(sections)} sections. Max ID: {section_max_id_known}")
    
    pool.close()
    await pool.wait_closed()

async def test_sphinx_connection():
    """Test connection to Sphinx RT index"""
    try:
        async with sphinx_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT COUNT(*) FROM releases_rt LIMIT 1")
                result = await cur.fetchone()
                logger.info(f"Sphinx connection test successful. Index contains {result[0]} records.")
                return True
    except Exception as e:
        logger.error(f"Sphinx connection test failed: {e}")
        return False

if __name__ == "__main__":
    async def startup():
        """Initialize everything and start the main loop"""
        logger.info("Starting Sphinx Binlog Updater...")
        
        # Initialize Sphinx connection pool
        await init_sphinx_pool()
        
        # Test Sphinx connection
        if not await test_sphinx_connection():
            logger.error("Failed to connect to Sphinx. Exiting.")
            return
        
        # Cache groups and sections
        await fetch_all_groups()
        await fetch_all_sections()
        
        if not groups or not sections:
            logger.error("Failed to cache groups or sections. Exiting.")
            return
        
        logger.info("Initialization complete. Starting binlog monitoring...")
        await main()
    
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(startup())
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
