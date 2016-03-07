Script and associated files necessary to load a set of records for
processing by the Python [dedupe](http://dedupe.readthedocs.org)
library, with a goal of deduping, or identifying matching records.

The ```pgdedupe.py``` script borrows heavily from the
[pgsql_big_dedupe_example.py](http://datamade.github.io/dedupe-examples/docs/pgsql_big_dedupe_example.html)
example script with some modifications.  It has been debugged on a
sample set of OSHA inspection records.

This script was developed on OS X 10.11.3 using Anaconda and Python
3.5.  On an ubuntu machine, you might want to have Anaconda installed
as well as the gcc and g++ system packages to successfully install
the dedupe library without heavy lifting.
