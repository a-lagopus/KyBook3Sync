#!/usr/bin/env python2
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)

__license__   = 'GPL v3'
__copyright__ = '2019, karl1c <karl1c@hotmail.com>'
__docformat__ = 'restructuredtext en'

from collections import OrderedDict
try:
    from PyQt5 import QtWidgets as QtGui
    from PyQt5.Qt import QWidget, QGridLayout, QLabel, QLineEdit, QCheckBox
except ImportError as e:
    from PyQt4 import QtGui
    from PyQt4.Qt import QWidget, QGridLayout, QLabel, QLineEdit, QCheckBox
from calibre.utils.config import JSONConfig

KEY_CONTENT_SERVER = 'content_server'
KEY_USERNAME = 'username'
KEY_PASSWORD = 'password'
KEY_FORMATS = 'formats'
KEY_REMOVE_HTML = 'remove_html'

# SHOW_REMOVE_HTML = OrderedDict([('no', 'No'),
                        # ('yes', 'Yes')])

DEFAULT_STORE_VALUES = {
    KEY_CONTENT_SERVER: 'http://192.168.1.1:8080',
    KEY_USERNAME: 'guest',
    KEY_PASSWORD: 'password',
    KEY_FORMATS: ['EPUB', 'PDF', 'MOBI', 'AZW3', 'AZW4', 'DJVU'],
    KEY_REMOVE_HTML: 0
}

# This is where all preferences for this plugin will be stored
# Remember that this name (i.e. plugins/KyBook3 Sync) is also
# in a global namespace, so make it as unique as possible.
# You should always prefix your config file name with plugins/,
# so as to ensure you dont accidentally clobber a calibre config file
prefs = JSONConfig('plugins/KyBook3 Sync')

# Set defaults
prefs.defaults = DEFAULT_STORE_VALUES

class ConfigWidget(QWidget):

    def __init__(self):
        QWidget.__init__(self)
        layout = QGridLayout(self)
        self.setLayout(layout)

        c = prefs

        layout.addWidget(QLabel('Link (from KyBook 3\'s content server):', self), 2, 0, 1, 2)
        text = c.get(KEY_CONTENT_SERVER, DEFAULT_STORE_VALUES[KEY_CONTENT_SERVER])
        self.c_s_ledit = QLineEdit(text, self)
        layout.addWidget(self.c_s_ledit, 3, 0, 1, 2)

        layout.addWidget(QLabel('Username (from KyBook3\'s content server):', self), 4, 0, 1, 2)
        text = c.get(KEY_USERNAME, DEFAULT_STORE_VALUES[KEY_USERNAME])
        self.username_ledit = QLineEdit(text, self)
        layout.addWidget(self.username_ledit, 5, 0, 1, 2)

        layout.addWidget(QLabel('Password (from KyBook3\'s content server):', self), 6, 0, 1, 2)
        text = c.get(KEY_PASSWORD, DEFAULT_STORE_VALUES[KEY_PASSWORD])
        self.password_ledit = QLineEdit(text, self)
        layout.addWidget(self.password_ledit, 7, 0, 1, 2)

        layout.addWidget(QLabel('File formats you wish to sync (comma separated, as they appear under Formats in Calibre\'s side bar):', self), 8, 0, 1, 2)
        formats = c.get(KEY_FORMATS, DEFAULT_STORE_VALUES[KEY_FORMATS])
        self.formats_ledit = QLineEdit(','.join(formats), self)
        layout.addWidget(self.formats_ledit, 9, 0, 1, 2)

        self.html_checkbox = QCheckBox('Remove HTML from comments?', self)
        html = c.get(KEY_REMOVE_HTML, DEFAULT_STORE_VALUES[KEY_REMOVE_HTML])
        self.html_checkbox.setChecked(html)
        layout.addWidget(self.html_checkbox, 10, 0, 1, 2)

    def save_settings(self):
        prefs[KEY_CONTENT_SERVER] = unicode(self.c_s_ledit.text())
        prefs[KEY_USERNAME] = unicode(self.username_ledit.text())
        prefs[KEY_PASSWORD] = unicode(self.password_ledit.text())
        formats = unicode(self.formats_ledit.text()).replace(' ','')
        prefs[KEY_FORMATS] = formats.split(',')
        prefs[KEY_REMOVE_HTML] = self.html_checkbox.isChecked()
