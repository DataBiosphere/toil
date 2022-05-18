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
  file:
    type: File
expression: |
  ${
    for (var i = 0; i < inputs.dir.listing.length; i++) {
      if (inputs.dir.listing[i].basename == inputs.filename) {
        return {file: inputs.dir.listing[i]};
      }
    }
    throw new Error("File " + inputs.filename + " not found");
  }

