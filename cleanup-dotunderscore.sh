#!/bin/bash
# Remove ALL ._ (macOS resource fork) files and directories recursively
find . -name '._*' -print -delete
echo "Done."