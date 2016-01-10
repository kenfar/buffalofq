# BuffaloFQ - a method of moving files via a file system queue

The objective of BuffaloFQ is to provide a very simple and reliable method of
moving files around an architecture.

### Installation:
$ mkvirtualenv buffalofq_cpy
$ pip install buffalofq --upgrade

### Configuration:
Most configuration is handled through a simple yaml file, that is kept in the xdg-compliant config directory.  On linux this would be:

* $HOME/.config/buffalofq_mover/[config-name1].yml
* $HOME/.config/buffalofq_mover/[config-name2].yml
* $HOME/.config/buffalofq_mover/[config-name3].yml

Config example:

* name:               ids_to_load    #
* status:             enabled        # choices are: enabled, disabled
* polling_seconds:    60             # delay in seconds between checks for new files
* limit_total:        -1             # choices: -1 (run continuously), 0 (run until source_dir is empty), [some number] run until this number have been moved.
* port:               22             #
* key_filename:       id_auto        # your ssh key
* log_dir:            /data/logs     # location buffalofq_mover will write its logs
* log_level:          debug          # choices are: info, warning, error, critical
* sort_key:           time           # choices are: None, name, or name of a field within filename
* source_host:        localhost      # must be localhost at this time
* source_user:        bobsmith       # not yet used
* source_dir:         /data/output   #
* source_fn:          '*'            # wild-card for selecting source files
* source_post_dir:    /data/archive  #
* source_post_action: move           # choices: move, delete, pass
* dest_host:          datawarehouse  #
* dest_user:          bobsmith       # used to log into dest_host
* dest_dir:           /data/input    #
* dest_fn:            ''             # needed if dest_post_action == symlink
* dest_post_dir:      None           # not used yet
* dest_post_action:   pass           # choices: pass, symlink


### Run:

To run once, you can simply run it like:

* $ ./buffalofq_mover --config-name [config-name1]

A trivial way to keep it running continuously:

* $ nohup ./buffalofq_mover --config-name [config-name1] &

