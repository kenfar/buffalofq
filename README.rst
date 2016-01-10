BuffaloFQ - a method of moving files via a file system queue
============================================================

The objective of BuffaloFQ is to provide a very simple and reliable
method of moving files around an architecture.

Installation:
~~~~~~~~~~~~~

$ mkvirtualenv buffalofq\_cpy $ pip install buffalofq --upgrade

Configuration:
~~~~~~~~~~~~~~

Most configuration is handled through a simple yaml file, that is kept
in the xdg-compliant config directory. On linux this would be:

-  $HOME/.config/buffalofq\_mover/[config-name1].yml
-  $HOME/.config/buffalofq\_mover/[config-name2].yml
-  $HOME/.config/buffalofq\_mover/[config-name3].yml

Config example:

-  name: ids\_to\_load #
-  status: enabled # choices are: enabled, disabled
-  polling\_seconds: 60 # delay in seconds between checks for new files
-  limit\_total: -1 # choices: -1 (run continuously), 0 (run until
   source\_dir is empty), [some number] run until this number have been
   moved.
-  port: 22 #
-  key\_filename: id\_auto # your ssh key
-  log\_dir: /data/logs # location buffalofq\_mover will write its logs
-  log\_level: debug # choices are: info, warning, error, critical
-  sort\_key: time # choices are: None, name, or name of a field within
   filename
-  source\_host: localhost # must be localhost at this time
-  source\_user: bobsmith # not yet used
-  source\_dir: /data/output #
-  source\_fn: '\*' # wild-card for selecting source files
-  source\_post\_dir: /data/archive #
-  source\_post\_action: move # choices: move, delete, pass
-  dest\_host: datawarehouse #
-  dest\_user: bobsmith # used to log into dest\_host
-  dest\_dir: /data/input #
-  dest\_fn: '' # needed if dest\_post\_action == symlink
-  dest\_post\_dir: None # not used yet
-  dest\_post\_action: pass # choices: pass, symlink

Run:
~~~~

To run once, you can simply run it like:

-  $ ./buffalofq\_mover --config-name [config-name1]

A trivial way to keep it running continuously:

-  $ nohup ./buffalofq\_mover --config-name [config-name1] &

