# KyBook3Sync
This plugin provides master-slave synchronisation of books and metadata from Calibre to KyBook3 (http://kybook-reader.com).
ADDED:Kybook3 app for iOS

KyBook3 provides support for almost every book and audiobook format and can read metadata from them. However, many files have incorrect metadata and/or cover. Even when KyBook3 downloads metadata and covers from Calibre's Content Server some metadata and cover errors occur. This plugin is an attempt to overcome these shortcomings.

Main Features:

Upload selected books to KyBook3 (skips files with the same MD5 already in KyBook3)
Sync title, authors, publishers, subjects, series, publication date, language, comments, ratings, (some) identifiers, & cover to KyBook3 with the following mapping:
Calibre Tags -> KyBook3 Subjects
Calibre Comments -> KyBook3 Annotations
Backup of KyBook3's metadata


Special Notes:

The plugin assumes that Calibre's metadata is more correct than KyBook3's
Neither books nor metadata in Calibre are changed.
No book files in KyBook3 will be changed or overwritten.
Metadata in KyBook3 is overwritten by that in Calibre
You should probably ensure that Calibre's Tags are single words with an initial capital
Calibre's Comments can (optionally) have HTML stripped
There is currently no way to sync from KyBook3 to Calibre (although this is planned)
Sync takes place over Wi-Fi, so may take a long time. I suggest you sync in small batches. Alternatively, you can add all the books you want in KyBook3 from iTunes and then run KyBook3 Sync to sync just the metadata


Testimonials:

"... syncing calibre with an actual reader app is a dream come true, thank you."

Installation Steps:

Download the attached zip file and install the plugin/restart Calibre/add to context menu or toolbar as described in the Introduction to plugins thread
Click on the KyBook3 Sync icon to see KyBook3's menu
Click on "Customize this plugin" in KyBook3's menu and set the Link, Username, & Password used by KyBook3's Content Server
You should now be able to select some books in Calibre and sync them to KyBook3


Debugging:
If you have problems with the plugin:

Be patient, syncing over WiFi can be slow and if your first book is a 500MB PDF, progress will be "stuck" at 1% for a long time!
In Calibre click Preferences | Restart in debug mode
After the restart, try running the plugin again
When it fails, or it appears to get stuck, close Calibre
A text file will appear with debug info, copy the contents and put into a GitHub Issue.
