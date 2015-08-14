from __future__ import absolute_import
import os
import sys
import ast
import tempfile
import shutil


def enable_absolute_imports(script, script_name):
    """
    Empty modules
    >>> enable_absolute_imports('')
    'from __future__ import absolute_import\\n'

    Ignore empty lines
    >>> enable_absolute_imports('\\n')
    'from __future__ import absolute_import\\n'

    Append after initial comments, like shebangs
    >>> enable_absolute_imports('#foo\\n')
    '#foo\\nfrom __future__ import absolute_import\\n'

    Insert before regular comments
    >>> enable_absolute_imports('#foo\\nimport bar\\n')
    '#foo\\nfrom __future__ import absolute_import\\nimport bar\\n'

    Insert before non-import statements
    >>> enable_absolute_imports('if False:\\n    pass\\n')
    'from __future__ import absolute_import\\nif False:\\n    pass\\n'

    Idempotence
    >>> enable_absolute_imports('from __future__ import absolute_import\\n') is None
    True

    Other __future__ imports
    >>> enable_absolute_imports('from __future__ import print_function\\n')
    'from __future__ import absolute_import\\nfrom __future__ import print_function\\n'

    Insert before from ... immport statements
    >>> enable_absolute_imports('from blah import fasel\\n')
    'from __future__ import absolute_import\\nfrom blah import fasel\\n'

    Insert before multiple future imports
    >>> enable_absolute_imports('from __future__ import print_function\\nfrom __future__ import nested_scopes\\n')
    'from __future__ import absolute_import\\nfrom __future__ import print_function\\nfrom __future__ import nested_scopes\\n'

    Insert before wrapped multi-name future import
    >>> enable_absolute_imports('from __future__ import (print_function,\\n    nested_scopes)\\n')
    'from __future__ import absolute_import\\nfrom __future__ import (print_function,\\n    nested_scopes)\\n'

    Usually docstrings show up as attributes of other nodes but unassociated docstring become
    Expr nodes in the AST.
    >>> enable_absolute_imports("#foo\\n\\n'''bar'''\\n\\npass")
    "#foo\\n\\nfrom __future__ import absolute_import\\n'''bar'''\\n\\npass\\n"

    Unassociated multiline docstring
    >>> enable_absolute_imports("#foo\\n\\n'''bar\\n'''\\n\\npass")
    "#foo\\n\\nfrom __future__ import absolute_import\\n'''bar\\n'''\\n\\npass\\n"
    """
    tree = ast.parse(script, filename=script_name)
    lines = script.split('\n')
    while lines and lines[-1] == "":
        lines.pop()
    node = None
    for child in ast.iter_child_nodes(tree):
        if isinstance(child, ast.Import):
            node = child
            break
        elif isinstance(child, ast.ImportFrom):
            assert child.level == 0  # don't know what this means
            if child.module == '__future__':
                if any(alias.name == 'absolute_import' for alias in child.names):
                    return None
                else:
                    if node is None: node = child
            else:
                node = child
                break
    if node is None:
        if len(tree.body) == 0:
            node = ast.stmt()
            node.lineno = len(lines) + 1
        else:
            node = tree.body[0]
            # This crazy heuristic tries to handle top-level docstrings with newlines in them
            # for which lineno is the line where the docstring ends
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Str):
                node.lineno -= node.value.s.count('\n')

    line = 'from __future__ import absolute_import'
    lines.insert(node.lineno - 1, line)
    lines.append("")
    return '\n'.join(lines)


def main(root_path):
    for dir_path, dir_names, file_names in os.walk(root_path):
        for file_name in file_names:
            if file_name.endswith('.py') and file_name != 'setup.py':
                file_path = os.path.join(dir_path, file_name)
                with open(file_path) as file:
                    script = file.read()
                new_script = enable_absolute_imports(script, file_name)
                if new_script is not None:
                    temp_handle, temp_file_path = tempfile.mkstemp(prefix=file_name, dir=dir_path)
                    try:
                        with os.fdopen(temp_handle, 'w') as temp_file:
                            temp_file.write(new_script)
                    except:
                        os.unlink(temp_file_path)
                        raise
                    else:
                        shutil.copymode(file_path,temp_file_path)
                        os.rename(temp_file_path, file_path)


if __name__ == '__main__':
    main(sys.argv[1])
