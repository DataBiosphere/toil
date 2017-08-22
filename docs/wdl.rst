WDL Support in Toil
********

How to Run toilwdl.py
-----------
WDL files all require json files to accompany them.  To run a workflow, simply run:

```python toilwdl.py wdlfile.wdl jsonfile.json```

This will create a folder, toil_outputs, with all of the outputs of the current workflow.

WDL Specifications
----------
WDL Language specifications can be found here: https://github.com/broadinstitute/wdl/blob/develop/SPEC.md

Implementing support for more features is currently underway, but a basic roadmap so far is:

CURRENTLY IMPLEMENTED:
 * scatter over tsv
 * handles calls, priority, and output file wrangling
 * handles single commands on the commandline
 * currently handles: $primitive_types & $array_types

TO BE IMPLEMENTED SOON:
 * handle docker usage
 * handle inserting memory requirements
 * implement type: $type_postfix_quantifier
 * "default" values inside variables
 * Alternative heredoc syntax ('>>>' & '<<<')
 * read_csv(), read_json(), ... etc.
 * $map_types & $object_types
 * wdl files that "import" other wdl files (including URI handling for 'http://' and 'https://')
