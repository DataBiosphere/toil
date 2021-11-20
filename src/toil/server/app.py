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
import logging
import os

import connexion  # type: ignore

from toil.lib.misc import get_public_ip
from toil.server.wes.toil_backend import ToilBackend
from toil.server.wsgi_app import run_app
from toil.version import version

logger = logging.getLogger(__name__)


def parser_with_server_options() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Toil server mode.")

    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--host", type=str, default="127.0.0.1",
                        help="The host interface that the Toil server binds on. (default: '127.0.0.1').")
    parser.add_argument("--port", type=int, default=8080,
                        help="The port that the Toil server listens on. (default: 8080).")
    parser.add_argument("--swagger_ui", action="store_true", default=False,
                        help="If True, the swagger UI will be enabled and hosted on the "
                             "`{api_base_path}/ui` endpoint. (default: False)")
    # CORS
    parser.add_argument("--cors", action="store_true", default=False,
                        help="Enable Cross Origin Resource Sharing (CORS). This should only be turned on "
                             "if the server is intended to be used by a website or domain.")
    parser.add_argument("--cors_origins", type=str, default="*",
                        help="Ignored if --cors is False. This sets the allowed origins for CORS. "
                             "For details about CORS and its security risks, see: "
                             "https://w3id.org/ga4gh/product-approval-support/cors. (default: '*')")
    # production only
    parser.add_argument("-w", "--workers", dest='workers', type=int, default=2,
                        help="Ignored if --debug is True. The number of worker processes launched by the "
                             "WSGI server. (default: 2).")

    parser.add_argument("--work_dir", type=str, default=os.path.join(os.getcwd(), "workflows"),
                        help="The directory where workflows should be stored. This directory should be "
                             "empty or only contain previous workflows. (default: './workflows').")
    parser.add_argument("--opt", "-o", type=str, action="append",
                        help="Specify the default parameters to be sent to the workflow engine for each "
                             "run.  Accepts multiple values.\n"
                             "Example: '--opt=--logLevel=CRITICAL --opt=--workDir=/tmp'.")
    parser.add_argument("--version", action='version', version=version)
    return parser


def create_app(args: argparse.Namespace) -> "connexion.FlaskApp":
    """
    Create a "connexion.FlaskApp" instance with Toil server configurations.
    """
    flask_app = connexion.FlaskApp(__name__,
                                   specification_dir='api_spec/',
                                   options={"swagger_ui": args.swagger_ui})

    flask_app.app.config['JSON_SORT_KEYS'] = False

    if args.cors:
        # enable cross origin resource sharing
        from flask_cors import CORS  # type: ignore
        CORS(flask_app.app, resources={r"/ga4gh/*": {"origins": args.cors_origins}})

    # add workflow execution service (WES) API endpoints
    backend = ToilBackend(work_dir=args.work_dir,
                          options=args.opt,
                          base_url=f"http://{get_public_ip()}:{args.port}")

    flask_app.add_api('workflow_execution_service.swagger.yaml',
                      resolver=connexion.Resolver(backend.resolve_operation_id))  # noqa

    # add custom endpoints
    if isinstance(backend, ToilBackend):
        base_url = "/toil/wes/v1"
        flask_app.app.add_url_rule(f"{base_url}/logs/<run_id>/stdout", view_func=backend.get_stdout)
        flask_app.app.add_url_rule(f"{base_url}/logs/<run_id>/stderr", view_func=backend.get_stderr)

    return flask_app


def start_server(args: argparse.Namespace) -> None:
    """ Start a Toil server."""
    flask_app = create_app(args)

    host = args.host
    port = args.port

    if args.debug:
        flask_app.run(host=host, port=port)
    else:
        # start a production WSGI server
        run_app(flask_app.app, options={
            "bind": f"{host}:{port}",
            "workers": args.workers,
        })
