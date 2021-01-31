#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)

__license__   = 'GPL v3'
__copyright__ = '2011, Grant Drake <grant.drake@gmail.com>'
__docformat__ = 'restructuredtext en'

import sys, time
from threading import Event
from threading import Thread
from multiprocessing.connection import Listener

from calibre.gui2.convert.single import sort_formats_by_preference
from calibre.gui2.threaded_jobs import ThreadedJob
from calibre.utils.config import prefs as cal_prefs
from calibre.utils.ipc.server import Server
from calibre.utils.ipc.job import ParallelJob
from calibre.utils.logging import Log
from calibre.constants import DEBUG

from calibre_plugins.kybook3_sync.config import prefs
import calibre_plugins.kybook3_sync.cal2ky3 as cal2ky3

# ------------------------------------------------------------------------------
#
#              Functions to perform sync using ThreadedJob
#
# ------------------------------------------------------------------------------

def start_sync_threaded(gui, ids, db, callback):
    '''
    This approach to syncing uses an in-process Thread to
    perform the work. This offers high performance, but suffers from
    memory leaks in the Calibre conversion process and will make the
    GUI less responsive for large numbers of books.

    '''
    job = ThreadedJob('KyBook3 Sync plugin',
            _('Sync %d books')%len(ids),
            sync_threaded, (gui, ids, db), {}, callback)
    gui.job_manager.run_threaded_job(job)
    gui.status_bar.show_message(_('KyBook3 Sync started'), 3000)


def sync_threaded(gui, ids, db, log=None, abort=None, notifications=None):
    '''
    In combination with start_sync_threaded this function performs
    the sync of the book(s) from a separate thread.
    '''
    library_path = cal_prefs['library_path']
    content_server = prefs['content_server']
    log_level = None
    if DEBUG:
        log_level = 'debug'
    username = prefs['username']
    password = prefs['password']
    formats_to_sync = prefs['formats']
    remove_html = prefs['remove_html']
    synced_ids = []
    failed_ids = list()
    no_format_ids = list()
    books = []
    for book_id in ids:
        if abort.is_set():
            log.error('Aborting ...')
            break
        # We need a blank path because to stop duplication in cal2ky3.py
        meta_dic = {"id": book_id, "path": ''}
        # Get the current metadata for this book from the db
        mi = db.get_metadata(book_id, get_cover=True, cover_as_data=True)
        title, formats = mi.title, mi.formats
        if not any(fmt in formats for fmt in formats_to_sync):
            log.error('  No files of the required types available for', title)
            failed_ids.append((book_id, title))
            no_format_ids.append((book_id, title))
        else:
            # with open('/tmp/coverkarlic.jpg', 'wb') as fyl:
            #     fyl.write(mi.cover_data[1])
            for key in mi:
                if hasattr(mi, key):
                    meta_dic[key] = getattr(mi, key)
            # These are the mi attributes
            # device_collections title author_sort_map user_categories user_metadata authors author_sort author_link_map series application_id pubdate tags formats comments identifiers uuid rating last_modified languages title_sort timestamp cover_data publisher
            # fmts = db.formats(book_id)
            # if not fmts:
            #     continue
            paths = []
            for fmt in formats:
            # if fmt in formats_to_sync:
                fmt = fmt.lower()
                # Get a python file object for the format. This will be
                # either  an in memory file or a temporary on disk file
                path = db.format_abspath(book_id, fmt)
                paths.append(path)
            meta_dic['paths'] = paths
            if meta_dic['paths']:
                books.append(meta_dic)
                # # Set metadata in the format
                # set_metadata(ffile, mi, fmt)
                # ffile.seek(0)
                # # Now replace the file in the calibre library with the updated
                # # file. We dont use add_format_with_hooks as the hooks were
                # # already run when the file was first added to calibre.
                # db.add_format(book_id, fmt, ffile, run_hooks=False)
    # main(content_server, download_dir, filename, log_level, password, remove_html, username)
    if books:
        notifications.put((0.01, 'Syncing KyBook3'))
        thread = Thread(target = cal2ky3.main,
                        args = (library_path, content_server, username, password,
                                remove_html, None, log_level, None, books))
        thread.daemon = True
        thread.start()
        address = ('localhost', 26564)
        keep_running = True
        while keep_running:
            listener = Listener(address, authkey=bytes('8c5960e57151c4a6f9f524f3'))
            conn = listener.accept()
            try:
                while True:
                    if conn.poll():
                        data = conn.recv()
                        if data == 'close':
                            keep_running = False
                            break
                        if data == 'no c_s':
                            gui.status_bar.show_message(_('No Content Server found!'), 3000)
                            failed_ids = []
                            for book in books:
                                failed_ids.append((book['id'], book['title']))
                            keep_running = False
                            break
                        notifications.put((data['count'] / data['total'],
                            _('%s %d of %d')%(data['pass'], data['count'], data['total'])))
                    else:
                        time.sleep(0.01)
            except EOFError:
                listener.close()
    log('Sync complete, with %d failures'%len(failed_ids))
    return (synced_ids, failed_ids, no_format_ids)

def get_job_details(job):
    '''
    Convert the job result into a set of parameters including a detail message
    summarising the success of the sync operation.
    '''
    synced_ids, failed_ids, no_format_ids = job.result
    if not hasattr(job, 'html_details'):
        job.html_details = job.details
    det_msg = []
    for i, title in failed_ids:
        if i in no_format_ids:
            msg = title + ' (No files)'
            det_msg.append(msg)
    if len(synced_ids) > 0:
        if det_msg:
            det_msg.append('----------------------------------')
        for i, title, in synced_ids:
            msg = '%s synced'%(title)
            det_msg.append(msg)

    det_msg = '\n'.join(det_msg)
    return synced_ids, failed_ids, det_msg
