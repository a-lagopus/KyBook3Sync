#!/bin/bash
find . -type f -name .DS_Store -delete && calibre-customize -b . && cp ~/Library/Preferences/calibre/plugins/KyBook3\ Sync.zip ../
