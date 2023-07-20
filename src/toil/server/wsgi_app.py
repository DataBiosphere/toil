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
from typing import Any, Dict, Optional

from gunicorn.app.base import BaseApplication  # type: ignore


class GunicornApplication(BaseApplication):  # type: ignore
    """
    An entry point to integrate a Gunicorn WSGI server in Python. To start a
    WSGI application with callable `app`, run the following code:

        WSGIApplication(app, options={
            ...
        }).run()

    For more details, see: https://docs.gunicorn.org/en/latest/custom.html
    """
    def __init__(self, app: object, options: Optional[Dict[str, Any]] = None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def init(self, *args: Any) -> None:
        pass

    def load_config(self) -> None:
        parser = self.cfg.parser()
        env_args = parser.parse_args(self.cfg.get_cmd_args_from_env())

        # TODO: also read from the Gunicorn config file?

        for key, value in {**vars(env_args), **self.options}.items():
            if key in self.cfg.settings and value is not None:
                self.cfg.set(key.lower(), value)

    def load(self) -> object:
        return self.application


def run_app(app: object, options: Optional[Dict[str, Any]] = None) -> None:
    """
    Run a Gunicorn WSGI server.
    """
    GunicornApplication(app, options=options).run()
