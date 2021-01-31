#!/usr/bin/env python2

""" Takes data from Calibre's database and puts it in KyBook 3's database.
    Rationale:
    1. Downloading books to KyBook 3 from Calibre's Content Server is slow
    2. KyBook 3 doesn't always download the correct metadata and/or cover

    Usage:
    1.  Use Calibre to set 'Tags' on the books to be imported to KyBook 3.
        These Tags will become Subjects in KyBook 3
"""

# Helper functions and imports required for this script.
from multiprocessing.connection import Client
import sys
import logging
import argparse
from argparse import ArgumentTypeError as err
import os
import sqlite3
import time
from datetime import datetime
import hashlib
import shutil
from io import BytesIO
# import ipdb
import httplib
from base64 import b64encode
import urllib
import mimetypes
import re
import tempfile
from PIL import Image, ImageFile

ImageFile.MAXBLOCK = 1048576

# ---------- Change these depending on your setup ---------- #
# Location of KyBook 3's database file on content server
KYB_DB_URL = '/$App/db.sqlite'
# Where to download KyBook 3's database file to
KYB_DB_FILE = os.path.join(tempfile.gettempdir(), 'db.sqlite')
# Tables that have collation that needs removing
COLLATION_TABLES = ['authors', 'publishers', 'subjects', 'sequences']
# Lookup tables used by KyBook 3. Currently same as above.
LOOKUP_TABLES = COLLATION_TABLES + ['ebookids']  # + ['collections']
# Values used to switch collation on and off
ON = ' COLLATE swiftCaseInsensitiveCompare'
OFF = ''
# Sizes for thumbnails in KyBook 3
THUMB_WIDTH = 74
THUMB_HEIGHT = 105
EBOOK_SCHEMES = {'isbn': '10', 'amazon': '15', 'asin': '15', 'oclc': '12'}
# ---------------------------------------------------------- #

LOG = logging.getLogger(__name__)
LOG_LEVELS = {'critical': logging.CRITICAL,
              'error': logging.ERROR,
              'warning': logging.WARNING,
              'info': logging.INFO,
              'debug': logging.DEBUG}


class Table(object):
    """ Representation of a database table.

        Used to get the table's name, col names, etc of a DB table.
    """

    def __init__(self, name):
        """ Initialize the properties dependent on the name of the table. """
        # Set the defaults
        self.name = name
        self.xid = self.name[0] + 'id'
        self.maincol = self.name[:-1]   # The col after [x]id col
        self.midcols = self.maincol     # The cols between [x]id/timestamp
        self.namecol = self.maincol     # Usually singular of table's name
        self.extra_sql = ''             # Extra sql needed to create table
        self.questions = '?, ?'         # Placeholders needed for inserts
        self.calname = self.name        # Equivalent tablename in Calibre
        self.calmain = self.maincol
        # Make specific where required
        if self.name == 'authors':
            self.maincol = 'namekey'
            # TODO: consider removing ebookid so it gets left alone
            self.midcols = 'namekey, name'
            # self.midcols = 'namekey, name, ebookid'
            self.namecol = 'name'
            self.extra_sql = ("""
    name TEXT NOT NULL,
    ebookid TEXT,""")
            self.questions = '?, ?, ?'
            # self.questions = '?, ?, ?, ?'
        elif self.name == 'publishers':
            pass
        elif self.name == 'subjects':
            self.calname = 'tags'
            self.calmain = 'tag'
        elif self.name == 'sequences':
            self.xid = 'qid'
            self.extra_sql = ("""
    ebookid TEXT,""")
            # TODO: consider removing ebookid so it gets left alone
            self.midcols = 'sequence'
            # self.midcols = 'sequence, ebookid'
            self.questions = '?, ?'
            # self.questions = '?, ?, ?'
            self.calname = 'series'
            self.calmain = 'series'
        elif self.name == 'collections':
            self.xid = 'lid'
        elif self.name == 'ebookids':
            self.midcols = 'scheme, value'
            self.namecol = 'value'
            self.questions = '?, ?, ?'
            self.calname = 'identifiers'

    def __getattr__(self, name):
        """ Automatically return attributes (self.xid, self.name, etc)
            without having to specifically code the getters.
        """
        return self.name


class Database(object):
    """ Implements a driver for an sqlite3 database. """

    def __init__(self, path):
        self.open(path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        if isinstance(exc_val, Exception):
            self.connection.rollback()
        else:
            self.commit()
        self.close()

    @property
    def connection(self):
        """ Connection to the database. """
        return self._conn

    @property
    def cursor(self):
        """ The connection's cursor. """
        return self._cursor

    def open(self, path):
        """ Open a connection to a sqlite database file """
        self._conn = sqlite3.connect(path)
        # self._conn.set_trace_callback(print)  # So we can log actual SQL
        self._conn.row_factory = sqlite3.Row  # So we can index by col name
        self._cursor = self._conn.cursor()

    def execute(self, sql, params=None, log_result=False):
        """ Execute an SQL query. """
        saved_sql = sql
        LOG.debug(saved_sql)
        # if params and len(params) > 0:
        if params:
            temp_sql = "SELECT " + ", ".join(["quote(?)" for param in params])
            self.cursor.execute(temp_sql, params)
            quoted_values = self.cursor.fetchone()
            for quoted_value in quoted_values:
                saved_sql = re.sub(r'(VALUES\(|, | = | LIKE )\?',
                                   r'\g<1>' + unicode(quoted_value),
                                   saved_sql, 1)
        LOG.debug(saved_sql)
        self.cursor.execute(sql, params or ())
        if log_result:
            LOG.debug('Row count: %s', self.cursor.rowcount)
            if self.cursor.rowcount == 1:
                LOG.debug('OK')
            elif not self.cursor.rowcount == -1:
                LOG.debug('Failed!')

    def executemany(self, sql, params=None):
        """ Executemany an SQL query. """
        self.cursor.executemany(sql, params or [])

    def commit(self):
        """ Commit changes to the database. """
        self.connection.commit()

    def query(self, sql, params=None):
        """ Run an SQL query. """
        self.execute(sql, params or (), log_result=True)
        return self.fetchall()

    def fetchone(self):
        """ Fetch one result. """
        return self.cursor.fetchone()

    def fetchall(self):
        """ Fetch all results. """
        all_rows = self.cursor.fetchall()
        LOG.debug('%d rows', len(all_rows))
        return all_rows

    def dump(self, filename):
        """ Emulate sqlite3 .dump command. """
        with open(filename, 'w') as fyl:
            for line in self.connection.iterdump():
                fyl.write('%s\n' % line.encode('utf-8'))

    def close(self):
        """ Close the connection """
        LOG.debug('Closing database')
        if self.connection:
            self.commit()
            self.cursor.close()
            self.connection.close()


class CalibreDB(Database):
    """ Implements a driver for Calibre's sqlite database."""

    def __init__(self, db_path, cal_data):
        super(CalibreDB, self).__init__(db_path)
        LOG.debug('Opening: %s', db_path)
        self._lib_path = os.path.dirname(db_path)
        self._cal_data = cal_data

    @property
    def cal_data(self):
        """ Allow access to self._cal_data """
        return self._cal_data

    def get_metadata(self):
        """ Select the metadata items from Calibre's DB that we need for
            KyBook 3's DB """
        # If we were called from the plugin we already have the data
        if self._cal_data:
            for cal_datum in self._cal_data:
                # Make cal_data match what we get directly from Calibre's DB
                # author_sort_map is a dict
                cal_datum['authors'] = cal_datum['author_sort_map'].items()
                # series is a string
                cal_datum['sequences'] = [cal_datum['series']]
                # tags are mapped directly to subjects
                cal_datum['subjects'] = cal_datum['tags']
                cal_datum['ebookids'] = cal_datum['identifiers'].items()
                # We only want the 1st 2 (lc) letters of 1st language
                if 'language' in cal_datum['languages']:
                    cal_datum['language'] = cal_datum['languages'][0][0:2].lower()
                else:
                    cal_datum['language'] = ''
                # publisher is a string
                cal_datum['publishers'] = [cal_datum['publisher']]
            return self._cal_data
        # SQL code to select the data from Calibre that needs to go to
        # KyBook 3.
        cal_metadata_sql = ("""SELECT id, title, pubdate,
(
    SELECT l.lang_code FROM languages as l
    JOIN books_languages_link as b_l_l on b_l_l.lang_code = l.id
    WHERE b_l_l.book = books.id
    GROUP BY b_l_l.book
) as language,
(
    SELECT text
    FROM comments
    WHERE book = books.id
) AS comments,
path,
last_modified
FROM books;""")
        return self.query(cal_metadata_sql)

    def get_books_files(self, b_id):
        """ Get the files associated with a book. """
        if self._cal_data:
            for cal_datum in self._cal_data:
                if cal_datum['id'] == b_id:
                    return cal_datum['paths']
        # SQL code to select the filename's associated with a book.
        books_files_sql = ("""SELECT name as filename, LOWER(format) as ext
FROM data
WHERE book = ?;""")
        return self.query(books_files_sql, (b_id,))

    def update(self):
        """ Update Calibre's DB with metadata from KyBook 3's DB. """
        # TODO: Consider adding this.
        pass

    def get_md5(self, path, row):
        """ Get the md5 hash of a book's file on disk.
            MD5 is used by KyBook and makes sure we are talking about the same
            file and, consequently, book."""
        md5 = ''
        b_file = self.path_from_row(path, row)
        md5 = hashlib.md5(open(b_file, 'rb').read()).hexdigest()
        LOG.debug('MD5 for %s: %s', b_file, md5)
        return md5

    def send_book_file_to_cs(self, c_s, path, row):
        """ Send a book's file to KyBook 3's content server."""
        b_file = self.path_from_row(path, row)
        # Upload to /Books/, same name, don't delete existing file.
        c_s.upload_file(b_file, '/Books/', remote_file=None,
                        del_existing=False)

    def path_from_row(self, path, row):
        """ Utility method to build a path from a row of data. """
        if self._cal_data:
            return row
        filename = row['filename']
        suffix = '.' + row['ext']
        return os.path.join(self._lib_path, path, filename + suffix)

    @staticmethod
    def mod_time(last_mod):
        """ Return the last_modified time as a (KyBook 3) timestamp """
        offset = datetime(2001, 1, 1)
        try:
            stamp = datetime.strptime(last_mod, "%Y-%m-%d %H:%M:%S+00:00")
        except ValueError:
            stamp = datetime.strptime(last_mod, "%Y-%m-%d %H:%M:%S.%f+00:00")
        LOG.debug('Calibre timestamp: %s', (stamp - offset).total_seconds())
        return (stamp - offset).total_seconds()


class KyBookDB(Database):
    """ Implements a driver for KyBook 3's sqlite database."""

    def __init__(self, db_path, remove_html, cal_lib_path):
        super(KyBookDB, self).__init__(db_path)
        LOG.debug('Opening: %s', db_path)
        self._remove_html = remove_html
        self._cal_lib_path = cal_lib_path
        LOG.debug('Collation tables: %s', COLLATION_TABLES)
        self._collation_tables = COLLATION_TABLES
        LOG.debug('Lookup tables: %s', LOOKUP_TABLES)
        self._lookup_tables = LOOKUP_TABLES

    def set_collation(self, on_or_off):
        """ Set the Collation procedure on or off.
            Used to allow writing to the tables without checking case.
            Build the SQL statement needed to enable/disable collation for the
            tables that require it.

            The statement has four places holders:
            0: the name of the table (table)
            1: name of the id column (3 letters: 1st of table name + id)
               (table[0])
            2: name of the main (data) column (singular of table name)
               (table[:-1])
            3: the collation phrase (to enable) or '' (to disable)
            4: authors table needs some AUTHORS_EXTRA_SQL (for name, ebookid
               cols)
        """
        set_collation_sql = ("""UPDATE SQLITE_MASTER
SET SQL = 'CREATE TABLE {0}
(
    {1} INTEGER NOT NULL PRIMARY KEY,
    {2} TEXT NOT NULL UNIQUE{3},{4}
    timestamp REAL NOT NULL
)' WHERE NAME = '{0}'""")
        self.execute("""PRAGMA writable_schema = 1;""", log_result=False)
        for table in self._collation_tables:
            tbl = Table(table)
            sql = set_collation_sql.format(tbl.name, tbl.xid, tbl.maincol,
                                           on_or_off, tbl.extra_sql)
            self.execute(sql, log_result=True)
        self.execute("""PRAGMA writable_schema = 0;""", log_result=False)

    def update(self, cal_db, row, md5):
        """ Separate out the columns from Calibre's metadata and use them to
            update KyBook 3's DB. """
        # SQL code to update KyBook 3's DB with metadata from Calibre.
        update_metadata_sql = ("""UPDATE metadata
SET title = ?,
    published = ?,
    language = ?,
    annotation = ?,
    thumbnail = ?,
    aspectratio = ?,
    coverhash = ?
WHERE bid = (
    SELECT bid
    FROM books
    WHERE md5 = ?);""")
        b_id = row['id']
        title = row['title']
        published = row.get('pubdate', '')
        language = row.get('language', '')
        if isinstance(published, datetime):
            published = row['pubdate'].isoformat()
            published = published[:10]
            # .strftime("%Y-%m-%d")
        else:
            # We probably only need the first 10 digits of date
            published = row['pubdate'][:10]
        annotation = row.get('comments', '')
        if self._remove_html:
            annotation = self._remove_html_markup(annotation)
        coverhash = None
        # TODO: Might need the coverhash here
        # In Calibre, each book can have multiple files (epub, pdf, mobi,
        # etc.) but in KyBook 3 there has to be a separate DB entry for
        # each file.
        # path = row['path']
        thumbnail, aspectratio = self._get_thumb(row)
        # WATCH OUT! md5 needs to be the last entry.
        update_data = (title, published, language, annotation, thumbnail,
                       aspectratio, coverhash, md5)
        LOG.info('Updating KyBook 3\'s database ...')
        self.execute(update_metadata_sql, update_data, log_result=True)
        self.commit()
        self._del_book_from_link_tables(md5)
        self._del_book_from_reviews(md5)
        self._ins_book_to_link_tables(cal_db, b_id, md5)
        self._ins_book_to_reviews(cal_db, b_id, md5)

    def clean_up(self):
        """ Clean up any spurious entries in the DB.

            Currently, this just deletes any rows in lookup tables that are
            not used, i.e., don't appear in the link tables. """
        # SQL code to delete from the lookup tables
        # E.g., 0 = subjects; 1 = s
        del_from_lookups_sql = ("""DELETE FROM {0}
    WHERE {1} NOT IN (SELECT {1} FROM books_{0});""")
        for lookup_table in LOOKUP_TABLES:
            LOG.info('Clearing unused entries from table <%s>', lookup_table)
            tbl = Table(lookup_table)
            sql = del_from_lookups_sql.format(tbl.name, tbl.xid)
            self.execute(sql, log_result=True)
            self.commit()
        # NOTE: we should NOT need to do clean ups on the reviews table

    def get_metadata(self):
        """ Get the metadata from KyBook 3's DB that we want to add to
            Calibre's DB
            Currently, this just provides the path for file downloads.
        """
        get_metadata_sql = ("""SELECT path,
            (
                SELECT md5
                FROM books
                WHERE bid = files.bid
            ) as md5
        FROM files;""")
        return self.query(get_metadata_sql)

    def send_cover_file_to_cs(self, c_s, file_path, file_row, md5):
        """ Send a book's cover file to KyBook 3's content server."""
        sel_bid_sql = ("""SELECT bid FROM books WHERE md5 = ?""")
        self.execute(sel_bid_sql, (md5,), log_result=True)
        row = self.fetchone()
        if row:
            if file_path:
                c_file = os.path.join(self._cal_lib_path, file_path,
                                      'cover.jpg')
            else:
                c_file = os.path.join(os.path.dirname(file_row), 'cover.jpg')
            cs_file = '$' + str(row['bid']) + '.jpg'
            LOG.debug('c_file: %s; cs_file: %s', c_file, cs_file)
            c_s.upload_file(c_file, '/$User/covers/', cs_file,
                            del_existing=True)

    def md5_exists(self, md5):
        """ Check whether an MD5 exists in the books table. """
        # SQL to check whether md5 is already in KyBook 3's DB
        md5_exists_sql = ("""SELECT EXISTS
(
    SELECT 1 FROM books WHERE md5 = ?
) as exist;""")
        self.execute(md5_exists_sql, (md5,), log_result=True)
        row = self.fetchone()
        LOG.debug(row['exist'])
        if row['exist']:
            return True
        return False

    def mod_time(self, md5):
        """ Get the timestamp (modification) of a book given its file's MD5 """
        timestamp_sql = ("""SELECT timestamp FROM books WHERE md5 = ?;""")
        self.execute(timestamp_sql, (md5,), log_result=True)
        row = self.fetchone()
        timestamp = row['timestamp']
        LOG.debug('KyBook timestamp: %s', timestamp)
        return timestamp

    def _get_thumb(self, row):
        """ Get a thumbnail of a book's cover.
            Using data from a row from Calibre's DB, we follow the path to the
            cover. Then we reduce to fit KyBook's required dimensions
            (74 x 105) and return it. """
        thumbnail = ''
        aspectratio = 0
        if row['path']:
            cover_file = os.path.join(self._cal_lib_path, row['path'],
                                      'cover.jpg')
        else:
            cover_file = os.path.join(os.path.dirname(row['paths'][0]),
                                      'cover.jpg')
        try:
            with open(cover_file, 'rb') as fyl:
                jpg_data = fyl.read()
            image = Image.open(BytesIO(jpg_data))
        except (IOError, UnboundLocalError):
            LOG.error('An error occurred opening the image: %s', cover_file)
        else:
            if (image.size[0] > THUMB_WIDTH) or (image.size[1] > THUMB_HEIGHT):
                reduced_image = self._reduce_image_size(image, THUMB_HEIGHT,
                                                        THUMB_WIDTH)
            else:
                reduced_image = image
            aspectratio = reduced_image.size[1] / (reduced_image.size[0] * 1.0)
            output = BytesIO()
            reduced_image.save(output, format='jpeg',
                               optimize=True, quality=85)
            thumbnail = output.getvalue()
        return sqlite3.Binary(thumbnail), aspectratio

    def _del_book_from_link_tables(self, md5):
        """ Delete a book's entries from designated link tables.
            Spurious entries could be created by KyBook 3, if data are taken
            from the book's file rather than Calibre's DB."""
        # SQL code to delete from the link tables (books_subjects, etc.)
        # E.g., 0 = subjects
        del_books_links_sql = ("""DELETE from books_{0}
WHERE bid = (SELECT bid FROM books WHERE md5 = ?);""")
        for lookup_table in self._lookup_tables:
            sql = del_books_links_sql.format(lookup_table)
            LOG.debug('Deleting book from %s ...', lookup_table)
            self.execute(sql, (md5,), log_result=True)
            self.commit()

    def _del_book_from_reviews(self, md5):
        """ Delete a book's entries from the reviews table.
            Spurious entries could be created by KyBook 3, if data are taken
            from the book's file rather than Calibre's DB."""
        # SQL code to delete from the link tables (books_subjects, etc.)
        # E.g., 0 = subjects
        sql = ("""DELETE from reviews
WHERE bid = (SELECT bid FROM books WHERE md5 = ?);""")
        LOG.debug('Deleting book from reviews ...')
        self.execute(sql, (md5,), log_result=True)
        self.commit()

    def _ins_book_to_reviews(self, cal_db, cal_bid, md5):
        sel_from_cal_sql = ("""SELECT rating FROM ratings
    WHERE id IN (SELECT id FROM books_ratings_link WHERE book = ?);""")
        ins_review_sql = ("""INSERT OR REPLACE INTO reviews (bid, rating, timestamp)
    VALUES((SELECT bid FROM books WHERE md5 = ?), ?, ?)
    """)
        if cal_db.cal_data:
            for cal_datum in cal_db.cal_data:
                if cal_datum['id'] == cal_bid:
                    rating = cal_datum['rating']
        else:
            rating = cal_db.query(sel_from_cal_sql, (cal_bid,))
        if not rating:
            return
        LOG.debug('Inserting rating: %s', rating)
        offset = datetime(2001, 1, 1)
        timestamp = str((datetime.now() - offset).total_seconds())
        self.execute(ins_review_sql, (md5, rating, timestamp), log_result=True)
        self.commit()

    def _ins_book_to_link_tables(self, cal_db, cal_bid, md5):
        """ Insert entries into designated link tables.
            Use this to add entries from Calibre."""
        # SQL code to select data for a book from Calibre's lookup tables
        # 0 = tags; 1 = tag
        sel_from_link_tables = ("""SELECT * FROM {0}
    WHERE id IN (SELECT {1} FROM books_{0}_link WHERE book = ?);""")
    #     # SQL code to insert into the lookup tables
    #     # E.g., 0 = subjects; 1 = subject; 2 = '?, ?'; 3 = subject
    #     fill_lookups_sql = ("""INSERT OR REPLACE INTO {0}({1}, timestamp)
    # SELECT {2} WHERE NOT EXISTS(SELECT 1 FROM {0} WHERE {3} = ?);""")
        # SQL code to insert into the lookup tables
        # E.g., 0 = subjects; 1 = subject; 2 = '?, ?'; 3 = subject
        fill_lookups_sql = ("""INSERT OR REPLACE INTO {0} ({1}, {2}, timestamp)
    VALUES((SELECT {1} FROM {0} WHERE {3} LIKE ?), {4});""")
        # SQL code to insert into the link tables (books_subjects, etc.)
        # E.g., 0 = subjects; 1 = s; 2 = subject
        ins_books_links_sql = ("""INSERT OR REPLACE INTO books_{0} (bid, {1})
    SELECT books.bid, {0}.{2}
    FROM books, {0}
    WHERE books.md5 = ?
    AND {0}.{3} = ?;""")
        seqnumber = None
        for lookup_table in self._lookup_tables:
            tbl = Table(lookup_table)
            if cal_db.cal_data:
                for cal_datum in cal_db.cal_data:
                    if cal_datum['id'] == cal_bid:
                        lookup_rows = cal_datum[tbl.name]
                        if tbl.name == 'sequences':
                            seqnumber = cal_datum.get('series_index')
                            seqnumber = int(seqnumber) if seqnumber else seqnumber
                        LOG.debug('lookup_rows %s', lookup_rows)
            else:
                if tbl.name == 'ebookids':
                    cal_sql = ("""SELECT * FROM identifiers WHERE book = ?""")
                else:
                    cal_sql = sel_from_link_tables.format(tbl.calname,
                                                          tbl.calmain)
                lookup_rows = cal_db.query(cal_sql, (cal_bid,))
            if not lookup_rows:
                continue
            for lookup_row in lookup_rows:
                LOG.debug('lookup_row %s', lookup_row)
                offset = datetime(2001, 1, 1)
                timestamp = str((datetime.now() - offset).total_seconds())
                kyb_sql_fil = fill_lookups_sql.format(tbl.name, tbl.xid,
                                                      tbl.midcols, tbl.namecol,
                                                      tbl.questions)
                if tbl.name == 'sequences':
                    kyb_sql_ins = ins_books_links_sql.format(tbl.name, tbl.xid + ', seqnumber',
                                                             tbl.xid + ', ' + str(seqnumber), tbl.namecol)
                else:
                    kyb_sql_ins = ins_books_links_sql.format(tbl.name, tbl.xid,
                                                             tbl.xid, tbl.namecol)
                if tbl.name == 'authors':
                    if not cal_db.cal_data:
                        lookup_row = [lookup_row['name'], lookup_row['sort']]
                    name = lookup_row[0]
                    extra = lookup_row[1]
                elif tbl.name == 'ebookids':
                    LOG.debug('lookup_row[0]: %s', lookup_row[0])
                    LOG.debug('lookup_row[1]: %s', lookup_row[1])
                    if not cal_db.cal_data:
                        lookup_row = [lookup_row['type'], lookup_row['value']]
                    extra = EBOOK_SCHEMES.get(lookup_row[0]) or '0'
                    name = lookup_row[1]
                else:
                    if not cal_db.cal_data:
                        lookup_row = lookup_row['name']
                    name = lookup_row
                    extra = 'extra'
                if not name or not extra:
                    continue
                sql_fil_data = (timestamp, )
                if tbl.name == 'authors':
                    sql_fil_data = (extra, name) + sql_fil_data
                    # sql_fil_data = (extra, name, None) + sql_fil_data
                elif tbl.name == 'ebookids':
                    sql_fil_data = (extra, name) + sql_fil_data
                elif tbl.name == 'sequences':
                    sql_fil_data = (name,) + sql_fil_data
                    # sql_fil_data = (name, None) + sql_fil_data
                else:
                    sql_fil_data = (name, ) + sql_fil_data
                sql_fil_data = (name, ) + sql_fil_data
                LOG.debug('sql_fil_data %s', sql_fil_data)
                self.execute(kyb_sql_fil, sql_fil_data, log_result=True)
                self.execute(kyb_sql_ins, (md5, name), log_result=True)
                self.commit()

    @staticmethod
    def _reduce_image_size(image, thumb_height, thumb_width):
        """ Reduce the size of an image proportionately.
            Image must be <= 105 high AND <= 74 wide, hence the if/else.
        """
        width = image.size[0]
        height = image.size[1]
        if height / width > 1.418918918918919:
            height = thumb_height
            reduction = (height / float(image.size[1]))
            width = int((float(image.size[0]) * float(reduction)))
        else:
            width = thumb_width
            reduction = (width / float(image.size[0]))
            height = int((float(image.size[1]) * float(reduction)))
        smaller_image = image.resize((width, height), Image.ANTIALIAS)
        return smaller_image

    @staticmethod
    def _remove_html_markup(string):
        ''' A quick and dirty attempt to remove HTML markup from comments.
            (KyBook 2, doesn't support HTML in annotations, so we might as
            well remove it.)
        '''
        tag = False
        quote = False
        out = ""
        if not string:
            return out
        for char in string:
            if char == '<' and not quote:
                tag = True
            elif char == '>' and not quote:
                tag = False
            elif (char in ('"', "'")) and tag:
                quote = not quote
            elif not tag:
                out = out + char
        return out


class ContentServer():
    """ Implements a driver for KyBook 3's content server."""

    def __init__(self, host, username, password):
        LOG.debug(locals())
        self._host = host
        self._username = username
        self._password = password
        LOG.info('Logging in to %s', self._host)
        resp = self._http_conn('GET', '/', None)
        LOG.info(resp.reason)
        # except Exception:
        #     LOG.error('Unable to connect to %s.', self._host)
        #     LOG.error('Are you sure you started the Content Server?')
        #     raise
        # else:
        self.create_path('$User/covers')

    def _http_conn(self, method, url, payload=None):
        LOG.debug(self._host)
        if payload:
            params = urllib.urlencode(payload)
        else:
            params = None
        auth = '%s:%s' % (self._username, self._password)
        credentials = b64encode(auth.encode('utf-8'))
        headers = {'Authorization': 'Basic %s' % credentials}
        http = httplib.HTTPConnection(self._host)
        http.request(method, url, params, headers)
        resp = http.getresponse()
        http.close()
        return resp

    def _post_multipart(self, url, fields, files, tries=5):
        LOG.debug('Number of tries left: %d', tries)
        if tries == 0:
            return None
        auth = '%s:%s' % (self._username, self._password)
        credentials = b64encode(auth.encode('utf-8'))
        content_type, body = self._encode_multipart_formdata(fields, files)
        headers = {'Authorization': 'Basic %s' % credentials}
        headers['Content-Type'] = content_type
        headers['Content-Length'] = str(len(body))
        LOG.debug(headers)
        try:
            http = httplib.HTTPConnection(self._host, timeout=30)
            # TODO: consider wrapping this in a thread with a timeout so we
            # don't get hangs
            http.request('POST', str(url), body, headers)
            resp = http.getresponse()
        except Exception as ex:
            LOG.debug(ex)
            time.sleep(1)
            return self._post_multipart(url, fields, files, tries - 1)
        finally:
            http.close()
        return resp

    def _encode_multipart_formdata(self, fields, files):
        limit = '-----------------------------'
        num = str(int((datetime.now() - datetime(1970, 1, 1)).total_seconds()))
        limit = limit + num
        crlf = '\r\n'
        lines = []
        for (key, value) in fields:
            lines.append('--' + limit)
            lines.append('Content-Disposition: form-data; name="%s"' % key)
            lines.append('')
            lines.append(value)
        for (key, filename, value) in files:
            lines.append('--' + limit)
            lines.append('Content-Disposition: form-data; name="%s"; filename="%s"' % (key, filename))
            lines.append('Content-Type: %s' % self._get_content_type(filename))
            lines.append('')
            lines.append(value)
        lines.append('--' + limit + '--')
        lines.append('')
        body = crlf.join(lines)
        content_type = 'multipart/form-data; boundary=%s' % limit
        return content_type, body

    @staticmethod
    def _get_content_type(filename):
        return mimetypes.guess_type(filename)[0] or 'application/octet-stream'

    def create_path(self, full_path):
        """ Create a (non-existent) folder on KyBook 3's content server.

        Args:
            path:   The full path and name of the folder to create.
        """
        path = ''
        for part in self._split_path(full_path):
            # We CANNOT use os.path.join here because Windows puts a \ not /
            # path = os.path.join(path, part)
            path = path + '/' + part
            if not self.dir_exists(path):
                LOG.info('Creating %s', path)
                payload = {'path': path}
                resp = self._http_conn('POST', '/create', payload)
                LOG.info(resp.reason)

    def list_path(self, path):
        """ List a folder on KyBook 3's content server.
            NOTE: Currently unused.
            See also dir_exists and file_exists
        """
        LOG.debug('Listing %s', path)
        LOG.debug('self._host: %s', self._host)
        resp = self._http_conn('GET', '/list?path=' + path, None)
        LOG.debug(resp.reason)
        LOG.debug(resp.status)
        return resp.status == 200

    def delete_path(self, path):
        """ Delete a path (file or folder) on KyBook 3's content server.

        Args:
            path:     The full path (and name of the file) to delete.
        """
        payload = {'path': path}
        LOG.debug('Deleting %s', path)
        resp = self._http_conn('POST', '/delete', payload)
        LOG.debug(resp.reason)
        return resp.status == 200

    def download_db_file(self, path, local_path):
        """ Get db.sqlite from KyBook's content server.
        """
        self.download_file(path, local_path)
        if os.path.isfile(local_path) and os.path.getsize(local_path) > 0:
            # We have a KyBook 3 database file, so back it up.
            backup = local_path + datetime.now().strftime("-%Y%m%d-%H%M%S")
            shutil.copyfile(local_path, backup)
            LOG.info('%s copied to %s', local_path, backup)
            return True
        # No database file, so we can't continue.
        LOG.critical("No file at %s or it is empty.", local_path)
        return False

    def upload_db_file(self, db_file):
        """ Add the remote dir and upload a DB file. """
        self.upload_file(db_file, '/$App/', remote_file=None,
                         del_existing=True)

    def download_file(self, remote_file, local_file):
        """ Download a file from KyBook 3's content server.
        """
        LOG.info('Downloading %s to %s', remote_file, local_file)
        resp = self._http_conn('GET', '/download?path=' + remote_file, None)
        LOG.info(resp.reason)
        if resp.status == 200:
            with open(local_file, 'wb') as fyl:
                fyl.write(resp.read())
            LOG.info('%s written to %s', remote_file, local_file)

    def upload_file(self, local_file, remote_dir, remote_file=None,
                    del_existing=False):
        """ Upload a file to KyBook 3's content server, with optional deletion
            of existing file with same name.
        """
        LOG.debug('del_existing: %s', del_existing)
        url = '/upload'
        file_handle = open(local_file, 'rb')
        contents = file_handle.read()
        if not remote_file:
            remote_file = str(os.path.basename(local_file))
        files = [('files[]', remote_file, contents)]
        data = [('path', remote_dir)]
        if del_existing:
            # We cannot use os.path.join here because Windows puts \ not /
            # path_to_delete = os.path.join(remote_dir, remote_file)
            path_to_delete = remote_dir + '/' + remote_file
            LOG.debug('path_to_delete: %s', path_to_delete)
            if self.file_exists(path_to_delete):
                LOG.debug('Deleting existing file at %s', path_to_delete)
                if self.delete_path(path_to_delete):
                    LOG.debug('OK')
                else:
                    LOG.debug('Failed!')
        LOG.info('Uploading %s to %s%s', local_file, remote_dir, remote_file)
        resp = self._post_multipart(url, data, files)
        if resp:
            LOG.info(resp.reason)
        else:
            LOG.info('Failed!')

    def file_exists(self, remote_file):
        """ Check file exists on KyBook 3's content server.
        """
        LOG.debug('Checking existence of %s', remote_file)
        resp = self._http_conn('HEAD', '/download?path=' + remote_file, None)
        LOG.debug(resp.reason)
        # Return True if file exists
        return resp.status == 200

    def dir_exists(self, remote_dir):
        """ Check directory exists on KyBook 3's content server.
        """
        LOG.debug('Checking existence of %s', remote_dir)
        resp = self._http_conn('HEAD', '/list?path=' + remote_dir, None)
        LOG.debug(resp.reason)
        # Return True if directory exists
        return resp.status == 200

    @staticmethod
    def _split_path(path):
        """ Split a path into its constituent parts.
            Used to create paths on KyBook's content server.
            (Paths have to be created incrementally.)
        """
        allparts = []
        while 1:
            parts = os.path.split(path)
            if parts[0] == path:  # sentinel for absolute paths
                allparts.insert(0, parts[0])
                break
            elif parts[1] == path:  # sentinel for relative paths
                allparts.insert(0, parts[1])
                break
            else:
                path = parts[0]
                allparts.insert(0, parts[1])
        return allparts


class PathType():
    """ Ensure the download_dir given on the command line is valid.

        (https://stackoverflow.com/questions/11415570/
        directory-path-types-with-argparse)
    """
    #pylint: disable=too-few-public-methods
    def __init__(self, exists=True, typ='file', dash_ok=True):
        '''exists:
                True: a path that does exist
                False: a path that does not exist, in a valid parent directory
                None: don't care
           typ: file, dir, symlink, None, or a function returning True for
                valid paths
                None: don't care
           dash_ok: whether to allow "-" as stdin/stdout'''

        assert exists in (True, False, None)
        assert typ in ('file', 'dir', 'symlink',
                       None) or hasattr(typ, '__call__')

        self._exists = exists
        self._type = typ
        self._dash_ok = dash_ok

    def __call__(self, string):
        #pylint: disable=too-many-branches
        if string == '-':
            # the special argument "-" means sys.std{in,out}
            if self._type == 'dir':
                raise err('standard i/o (-) not allowed as directory path')
            elif self._type == 'symlink':
                raise err('standard i/o (-) not allowed as symlink path')
            elif not self._dash_ok:
                raise err('standard i/o (-) not allowed')
        else:
            exist = os.path.exists(string)
            if self._exists:
                if not exist:
                    raise err("path does not exist: '%s'" % string)

                if self._type is None:
                    pass
                elif self._type == 'file':
                    if not os.path.isfile(string):
                        raise err("path is not a file: '%s'" % string)
                elif self._type == 'symlink':
                    if not os.path.symlink(string):  #pylint: disable=no-member
                        raise err("path is not a symlink: '%s'" % string)
                elif self._type == 'dir':
                    if not os.path.isdir(string):
                        raise err("path is not a directory: '%s'" % string)
                elif not self._type(string):
                    raise err("path not valid: '%s'" % string)
            else:
                if not self._exists and exist:
                    raise err("path exists: '%s'" % string)

                path = os.path.dirname(os.path.normpath(string)) or '.'
                if not os.path.isdir(path):
                    raise err("parent path is not a directory: '%s'" % path)
                elif not os.path.exists(path):
                    raise err("parent directory does not exist: '%s'" % path)

        return string


def parse_arguments():
    """ Parse the arguments. """
    parser = argparse.ArgumentParser(
        description=('Sync files and metadata from Calibre to KyBook3. '
                     'Currently, syncing is only one way, although files '
                     'only in KyBook 3 are (optionally) downloaded.'))
    parser.add_argument('library_path',
                        help='Path to Calibre\'s library, e.g., '
                             '"C:\\Users\\John\\Documents\\Calibre Library"'
                             ' or "/Users/john/Calibre Library"')
    parser.add_argument('content_server',
                        help='URL:port of KyBook\'s content server, e.g., '
                             'http://192.168.1.1:8080')
    parser.add_argument('username', help='username for the content server')
    parser.add_argument('password', help='password for the content server')
    parser.add_argument('-r', '--remove-html', help='remove HTML in comments',
                        action='store_true')
    # parser.add_argument('-t', '--trial-run', help='do NOT upload any files',
    #                     action='store_true')
    parser.add_argument('-d', '--download_dir',
                        help='Download directory for books not in Calibre',
                        type=PathType(exists=True, typ='dir', dash_ok='False'),
                        metavar='/download/dir/')
    parser.add_argument('-l', '--log_level', help='level of logging provided',
                        choices=['info', 'warning', 'error', 'critical',
                                 'debug'])
    parser.add_argument('-f', '--filename', help='filename to save log to',
                        metavar='filename.ext')
    parser.add_argument('-', '--cal-data', help=argparse.SUPPRESS)
    # Always print help if we don't have 4 args (script, server, user, & pass)
    if len(sys.argv) < 5:
        sys.argv.append('-h')
    arguments = parser.parse_args()
    for key, value in vars(arguments).items():
        LOG.info('arg: %s %s', key, value)
    return arguments


def main(library_path, content_server, username, password, remove_html,
         download_dir, log_level, filename, cal_data):
    """ Where the work is done."""
    # Handle uncaught exceptions
    sys.excepthook = handle_exception
    conn = None
    if cal_data:
        address = ('localhost', 26564)
        conn = Client(address, authkey=bytes('8c5960e57151c4a6f9f524f3'))
        if not log_level:
            log_level = 'debug'
            filename = os.path.join(tempfile.gettempdir(), 'KyBook3Sync.log')
    setup_logging(log_level, filename)
    # LOG.debug(library_path, content_server, username, password, remove_html,
    #           download_dir, log_level, filename)
    content_server = re.sub(r"https?://", '', content_server).rstrip('/')
    try:
        c_s = ContentServer(content_server, username, password)
    except Exception:
        LOG.info('Could not connect to the Content Server. Did you start it?')
        conn.send('no c_s')
        return
    cal_db = CalibreDB(os.path.join(library_path, 'metadata.db'), cal_data)
    iterate_cal_data(c_s, cal_db, 'File sync', remove_html, conn, library_path)
    cal_book_file_md5s = iterate_cal_data(c_s, cal_db, 'Metadata sync',
                                          remove_html, conn, library_path)
    cal_db.close()
    if download_dir:
        # new_books = []
        kyb_db = KyBookDB(KYB_DB_FILE, remove_html, library_path)
        kyb_data = kyb_db.get_metadata()
        for kyb_datum in kyb_data:
            if not kyb_datum['md5'] in cal_book_file_md5s:
                # new_book = []
                # We CANNOT use os.path.join because Windows puts \ not /
                remote_file = '/' + kyb_datum['path']
                local_file = os.path.basename(remote_file)
                local_file = os.path.join(download_dir, local_file)
                c_s.download_file(remote_file, local_file)
    with KyBookDB(KYB_DB_FILE, remove_html, library_path) as kyb_db:
        kyb_db.set_collation(ON)
        kyb_db.dump(KYB_DB_FILE + '_end.txt')
    conn.send({'pass': 'Uploading DB file', 'count': 0, 'total': 1})
    c_s.upload_db_file(KYB_DB_FILE)
    conn.send({'pass': 'Uploading DB file', 'count': 1, 'total': 1})
    if conn:
        conn.send('close')
        conn.close()
    print('To use the uploaded covers, clear the book covers cache.')
    print('In KyBook 3, tap Control | Cache | BOOK COVERS CACHE |'
          ' Clear space')
    print('Then close and re-open KyBook 3.')
    LOG.info('All done.')
    # return new_books[]


def handle_exception(exc_type, exc_val, exc_trace):
    """ Handle uncaught exceptions. """
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_val, exc_trace)
        return
    LOG.error("Uncaught exception", exc_info=(exc_type, exc_val, exc_trace))


def setup_logging(level, filename):
    """ Setup the logging environment.

        If no log level is set as an option, use the logger to print progress
        to the screen. The format ensures the screen is cleared between prints.
    """
    # logging.getLogger("requests").setLevel(logging.WARNING)
    # logging.getLogger("urllib3").setLevel(logging.WARNING)
    # ipdb.set_trace()
    log_level = LOG_LEVELS.get(level, logging.NOTSET)
    if log_level:
        logging.basicConfig(level=log_level, filename=filename,
                            format='%(asctime)s %(funcName)s %(levelname)s:'
                            ' %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    else:
        # \x1b[80D = start of line; \x1b[1A = up 1 line; \x1b[K = clear to end
        logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                            format='%(message)s')
        # format='\x1b[80D\x1b[1A\x1b[K\x1b[80D\x1b[1A\x1b[K'
        #        '\x1b[80D\x1b[1A\x1b[K%(message)s')
        # handler = logging.StreamHandler(stream=sys.stdout)
        # LOG.addHandler(handler)


def iterate_cal_data(c_s, cal_db, iteration, remove_html, conn, library_path):
    """ Iterate over Calibre's data.
        We need to go over them twice: once to upload files, then to update
        KyBook 3's DB. """
    cal_book_file_md5s = []
    if c_s.download_db_file(KYB_DB_URL, KYB_DB_FILE):
        with KyBookDB(KYB_DB_FILE, remove_html, library_path) as kyb_db:
            if iteration == 'File sync':
                kyb_db.dump(KYB_DB_FILE + '_start.txt')
            kyb_db.set_collation(OFF)
        kyb_db = KyBookDB(KYB_DB_FILE, remove_html, library_path)
        cal_data = cal_db.get_metadata()
        count = 0
        total = len(cal_data)
        LOG.info('Total no. of books to sync: %s', total)
        for cal_datum in cal_data:
            count = count + 1
            LOG.info('Processing %s/%s books: %s ...', count, total,
                     cal_datum['title'])
            LOG.debug('Book ID: %s', cal_datum['id'])
            cal_bid = cal_datum['id']
            cal_path = cal_datum['path']
            for file_row in cal_db.get_books_files(cal_bid):
                md5 = cal_db.get_md5(cal_path, file_row)
                if iteration == 'File sync':
                    if not kyb_db.md5_exists(md5):
                        cal_db.send_book_file_to_cs(c_s, cal_path, file_row)
                    else:
                        LOG.info('File already in KyBook 3.')
                elif iteration == 'Metadata sync':
                    cal_book_file_md5s.append(md5)
                    LOG.debug('Book MD5s list: %s', cal_book_file_md5s)
                    kyb_db.update(cal_db, cal_datum, md5)
                    kyb_db.send_cover_file_to_cs(c_s, cal_path, file_row, md5)
            if conn:
                conn.send({'pass': iteration, 'count': count, 'total': total})
        if iteration == 'File sync':
            LOG.info('Waiting for KyBook3 ...')
            total = 20
            for sec in range(1, total + 1):
                time.sleep(1)
                if conn:
                    conn.send({'pass': 'Waiting', 'count': sec,
                              'total': total})
            LOG.info('OK')
        if iteration == 'Metadata sync':
            kyb_db.clean_up()
        kyb_db.close()
    else:
        LOG.info('Failed to download the DB file from KyBook3')
        sys.exit(1)
    return cal_book_file_md5s


if __name__ == '__main__':
    ARGS = parse_arguments()
    main(**vars(ARGS))


# Maybe used for syncing in the future
# cal_mtime = cal_db.mod_time(cal_datum['last_modified'])
# -----------------------------------------------------------
# This is where we'd do the sync, but KyBook 3 doesn't update
# its timestamp :-( In the future, perhaps we could do it??
# if kyb_db.md5_exists(md5):
#     LOG.debug('%s > %s', cal_mtime, kyb_db.mod_time(md5))
#     if cal_mtime > kyb_db.mod_time(md5):
#         # TODO: We'll need to set the timestamp
#         kyb_db.update(cal_db, cal_datum, md5)
#       kyb_db.send_cover_file_to_cs(c_s,
#                                     cal_path, file_row, md5)
#     elif cal_mtime < kyb_db.mod_time(md5):
#         # TODO: We'll need to set the last_modified
#         cal_db.update()
# -----------------------------------------------------------
