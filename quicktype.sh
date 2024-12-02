#!bash

printf -- '%s\0' jsonschema/pipes/*.schema.json | xargs -0 \
    quicktype --visibility public -s schema -l rust --derive-debug -o src/types.rs 
