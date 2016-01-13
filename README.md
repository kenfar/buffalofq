# BuffaloFQ - a method of moving files via a file system queue

The objective of BuffaloFQ is to provide a very simple and reliable method of
moving files around an architecture.

### Background, Examples, and FAQ
Please see the Wiki:  https://github.com/kenfar/buffalofq/wiki

### Installation:
$ pip install buffalofq

### Configuration:
Most configuration is handled through a simple yaml file, that is kept in the xdg-compliant config directory.  On linux this would be:

* $HOME/.config/buffalofq_mover/[config-name1].yml
* $HOME/.config/buffalofq_mover/[config-name2].yml
* $HOME/.config/buffalofq_mover/[config-name3].yml

Config example:

* name:               ids_to_load    #
* status:             enabled        # choices are: enabled, disabled
* polling_seconds:    None           # delay in seconds between checks for new files, defaults to 300
* limit_total:        -1             # choices: -1 (run continuously), 0 (run until source_dir is empty), [some number] run until this number have been moved.
* port:               None           # defaults to 22
* key_filename:       None           # defaults to id_buffalofq_rsa
* log_dir:            /data/logs     # location buffalofq_mover will write its logs
* log_level:          None           # choices are: info, warning, error, critical, defaults to debug
* sort_key:           time           # choices are: None, name, or name of a field within filename, defaults to None
* source_host:        localhost      # must be localhost at this time
* source_user:        None           # not yet used, defaults to current userid
* source_dir:         /data/output   #
* source_fn:          '*'            # wild-card for selecting source files
* source_post_dir:    /data/archive  #
* source_post_action: move           # choices: move, delete, None
* dest_host:          datawarehouse  #
* dest_user:          None           # used to log into dest_host, defaults to current userid
* dest_dir:           /data/input    #
* dest_fn:            None           # needed if dest_post_action is symlink or move
* dest_post_dir:      None           # not used yet
* dest_post_action:   None           # choices: symlink, move, None


### Run:

To run once, you can simply run it like:

* $ ./buffalofq_mover --config-name [config-name1]

A trivial way to keep it running continuously:

* $ nohup ./buffalofq_mover --config-name [config-name1] &

