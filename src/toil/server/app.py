# Copyright (C) 2015-2021 Regents of the University of California
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import argparse
import connexion  # type: ignore

from toil.server.wes.toil_backend import ToilBackend
from toil.server.wsgi_app import run_app


def start_server(args: argparse.Namespace) -> None:
    """ Start a Toil server."""
    flask_app = connexion.FlaskApp(__name__,
                                   specification_dir='ga4gh_api_spec/',
                                   options={"swagger_ui": args.swagger_ui})

    if args.cors:
        # enable cross origin resource sharing
        from flask_cors import CORS  # type: ignore
        CORS(flask_app.app, resources={r"/ga4gh/*": {"origins": args.cors_origins}})

    # workflow execution service (WES) API
    backend = ToilBackend(args.opt)

    backend.register_wf_type("py", ["3.6", "3.7", "3.8", ])
    backend.register_wf_type("CWL", ["v1.0", "v1.1", "v1.2"])
    backend.register_wf_type("WDL", ["draft-2", "1.0"])

    flask_app.add_api('workflow_execution_service.swagger.yaml',
                resolver=connexion.Resolver(backend.resolve_operation_id))  # noqa

    if args.debug:
        flask_app.run(port=args.port)
    else:
        # start a production WSGI server
        run_app(flask_app.app, options={
            "bind": f"127.0.0.1:{args.port}",
            "workers": 2,
        })
