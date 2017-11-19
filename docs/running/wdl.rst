WDL Support in Toil
********

How to Run toilwdl.py
-----------
WDL files all require json files to accompany them.  To run a workflow, simply run:

```toil-wdl-runner wdlfile.wdl jsonfile.json -o wdl_working```

The wdl and json files are required.  The -o option specifies the output folder, and if not entered
this will use the current working directory.

WDL Specifications
----------
WDL Language specifications can be found here: https://github.com/broadinstitute/wdl/blob/develop/SPEC.md

Implementing support for more features is currently underway, but a basic roadmap so far is:

CURRENTLY IMPLEMENTED:
 * scatter
 * read_tsv, read_csv
 * docker calls
 * handles priority, and output file wrangling
 * currently handles primitives and Array[File]

TO BE IMPLEMENTED SOON:
 * implement type: $type_postfix_quantifier
 * "default" values inside variables
 * $map_types & $object_types
 * wdl files that "import" other wdl files (including URI handling for 'http://' and 'https://')
