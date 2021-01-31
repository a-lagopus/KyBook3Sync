#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)

__license__   = 'GPL v3'
__copyright__ = '2019, karl1c <karl1c@hotmail.com>'
__docformat__ = 'restructuredtext en'

if False:
    # This is here to keep my python error checker from complaining about
    # the builtin functions that will be defined by the plugin loading system
    # You do not need this code in your plugins
    get_icons = get_resources = None

from calibre.ebooks.metadata.meta import set_metadata
from calibre.gui2 import error_dialog, info_dialog, Dispatcher

from PyQt5.Qt import QDialog, QVBoxLayout, QPushButton, QMessageBox, QLabel

from calibre_plugins.kybook3_sync.config import prefs
from calibre_plugins.kybook3_sync.jobs import (start_sync_threaded, get_job_details)
import sys
sys.path.append('./')
# import requests

import calibre_plugins.kybook3_sync.cal2ky3 as cal2ky3

class KyBook3SyncDialog(QDialog):

    def __init__(self, gui, icon, do_user_config):
        QDialog.__init__(self, gui)
        self.gui = gui
        self.do_user_config = do_user_config

        # The current database shown in the GUI
        # db is an instance of the class LibraryDatabase from db/legacy.py
        # This class has many, many methods that allow you to do a lot of
        # things. For most purposes you should use db.new_api, which has
        # a much nicer interface from db/cache.py
        self.db = gui.current_db

        self.l = QVBoxLayout()
        self.setLayout(self.l)


        self.setWindowTitle('KyBook3 Sync')
        self.setWindowIcon(icon)

        self.label = QLabel('Please configure this plugin before synchronizing.')
        self.l.addWidget(self.label)

        self.synchronize_button = QPushButton(
            'Synchronize with KyBook3', self)
        self.synchronize_button.clicked.connect(self.synchronize)
        self.l.addWidget(self.synchronize_button)

        self.conf_button = QPushButton(
                'Configure this plugin', self)
        self.conf_button.clicked.connect(self.config)
        self.l.addWidget(self.conf_button)

        self.howto_button = QPushButton('How to use this plugin', self)
        self.howto_button.clicked.connect(self.howto)
        self.l.addWidget(self.howto_button)


        self.about_button = QPushButton('About', self)
        self.about_button.clicked.connect(self.about)
        self.l.addWidget(self.about_button)

        self.resize(self.sizeHint())

    def howto(self):
        # Get the howto text from a file inside the plugin zip file
        # The get_resources function is a builtin function defined for all your
        # plugin code. It loads files from the plugin zip file. It returns
        # the bytes from the specified file.
        #
        # Note that if you are loading more than one file, for performance, you
        # should pass a list of names to get_resources. In this case,
        # get_resources will return a dictionary mapping names to bytes. Names that
        # are not found in the zip file will not be in the returned dictionary.
        text = get_resources('howto.txt')
        QMessageBox.about(self, 'How to use the KyBook3 Sync plugin',
                text.decode('utf-8'))


    def about(self):
        # Get the about text from a file inside the plugin zip file
        # The get_resources function is a builtin function defined for all your
        # plugin code. It loads files from the plugin zip file. It returns
        # the bytes from the specified file.
        #
        # Note that if you are loading more than one file, for performance, you
        # should pass a list of names to get_resources. In this case,
        # get_resources will return a dictionary mapping names to bytes. Names that
        # are not found in the zip file will not be in the returned dictionary.
        text = get_resources('about.txt')
        QMessageBox.about(self, 'About the KyBook3 Sync plugin',
                text.decode('utf-8'))

    def synchronize(self):
        '''
        Set the metadata in the files in the selected book's records to
        match those in KyBook3.
        '''
        # Get currently selected books
        rows = self.gui.library_view.selectionModel().selectedRows()
        if not rows or len(rows) == 0:
            return error_dialog(self.gui, 'Cannot sync with KyBook3',
                                'You must select one or more books to sync.', show=True)
        # Map the rows to book ids
        book_ids = list(map(self.gui.library_view.model().id, rows))
        db = self.db.new_api
        start_sync_threaded(self.gui, book_ids, db, Dispatcher(self._syncs_complete))
        self.hide()
        # info_dialog(self, 'Synchronized',
        #         'Synchronized %d book(s) with KyBook3'%len(ids),
        #         show=True)

    def _syncs_complete(self, job):
        if job.failed:
            job.description = job.description + ". Did you start KyBook3's Content Server?"
            self.gui.job_exception(job, dialog_title='Failed to sync with KyBook3')
            return
        synced_ids, failed_ids, det_msg = get_job_details(job)
        self.gui.status_bar.show_message('KyBook3 Sync completed', 3000)


    def config(self):
        self.do_user_config(parent=self)
        # Apply the changes
        # self.label.setText(prefs['hello_world_msg'])

