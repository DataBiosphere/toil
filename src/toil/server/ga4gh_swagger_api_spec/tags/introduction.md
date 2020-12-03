This document describes the WES API and provides details on the specific endpoints, request formats, and response.  It is intended to provide key information for developers of WES-compatible services as well as clients that will call these WES services.

Use cases include:

+ "Bring your code to the data": a researcher who has built their own custom analysis can submit it to run on a dataset owned by an external organization, instead of having to make a copy of the data
+ Best-practices pipelines: a researcher who maintains their own controlled data environment can find useful workflows in a shared directory (e.g. Dockstore.org), and run them over their data

## Standards

The WES API specification is written in OpenAPI and embodies a RESTful service philosophy. It uses JSON in requests and responses and standard HTTP/HTTPS for information transport.