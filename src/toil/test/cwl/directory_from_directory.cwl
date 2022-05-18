#!/usr/bin/env cwl-runner
cwlVersion: v1.2
class: ExpressionTool
requirements:
  InlineJavascriptRequirement: {}
  LoadListingRequirement:
    loadListing: shallow_listing
inputs:
  dir:
    type: Directory
  filename:
    type: string
outputs:
  directory:
    type: Directory
expression: |
  ${
    for (var i = 0; i < inputs.dir.listing.length; i++) {
      if (inputs.dir.listing[i].basename == inputs.filename) {
        return {directory: inputs.dir.listing[i]};
      }
    }
    throw new Error("Directory " + inputs.filename + " not found");
  }

