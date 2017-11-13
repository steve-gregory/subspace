import logging
import os
import operator
import json

from ansible.cli.playbook import PlaybookCLI

from .playbook_options import PythonPlaybookOptions

try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()

default_logger = logging.getLogger(__name__)


class PythonPlaybookRunner(PlaybookCLI):
    """
    Overwrite key methods from 'ansible.cli.playbook.PlaybookCLI'
    - Remove all reliance on the actual _cli_ portion.
      - Gather options via PythonPlaybookOptions
    """

    inventory = None
    loader = None
    options = None
    extra_vars = None
    variable_manager = None

    @staticmethod
    def _convert_extra_vars_to_option(extra_vars_dict={}):
        """
        Because we are 'mocking a CLI', we need to convert:
        {...} --> ['{...}']

        This will allow `_play_prereqs` to handle the code
        as it would if passed from the command-line.
        After `_play_prereqs` has parsed the JSON/YAML option,
        `variable_manager.extra_args` will again return the {...} structure.
        """
        extra_vars_option = [json.dumps(extra_vars_dict)]
        return extra_vars_option

    @classmethod
    def _select_playbooks_from_path(cls, playbook_path, limit_playbooks=[]):
        """
        Input:
          - Path of playbooks directory
          - A list of playbooks to be run.
        Output:
          An ordered list of playbook files.
        """
        if not isinstance(playbook_path, basestring):
            raise TypeError(
                "Expected 'playbook_path' as string,"
                " received %s" % type(playbook_path))
        # Convert file path to list of playbooks:
        if not os.path.exists(playbook_path):
            raise ValueError("Could not find path: %s" % (playbook_path,))

        if os.path.isdir(playbook_path):
            playbook_list = cls._select_playbooks_from_dir(
                playbook_path, limit_playbooks)
        else:
            playbook_list = [playbook_path]
        return playbook_list

    @classmethod
    def _select_playbooks_from_dir(cls, playbook_dir, limit=[]):
        """
        Given a directory, return all `.yml` playbooks
        If a limit is specified, only include playbooks found in the limit.
        """
        return [playbook_path for playbook_path in cls._list_yml_files_in_dir(playbook_dir)
                if not limit or playbook_path.split('/')[-1] in limit]

    @classmethod
    def _list_yml_files_in_dir(cls, directory):
        """
        Walk the directory and retrieve each yml file.
        """
        files = []
        directories = list(os.walk(directory))
        directories.sort(cmp=operator.lt)
        for d in directories:
            a_dir = d[0]
            files_in_dir = d[2]
            files_in_dir.sort()
            if os.path.isdir(a_dir):
                for f in files_in_dir:
                    if os.path.splitext(f)[1] == ".yml":
                        files.append(os.path.join(a_dir, f))
        return files

    @classmethod
    def factory(
            cls, playbook_path, limit_playbooks=[],
            **runner_options):
        """
        This factory method can help select an arbitrary number of playbooks to be executed,
        without having to explicitly state each one.
        By default, all playbooks found in `playbook_path` will be executed.
        If `limit_playbooks` is passed, only those playbooks will be executed.

        Notes: * Playbook files are identified as ending in .yml
               * Playbooks are not executed until you have called SubspacePlaybookRunner.run()
        """
        playbooks = cls._select_playbooks_from_path(playbook_path, limit_playbooks)
        playbook_runner = cls(
            playbooks,
            **runner_options
        )
        return playbook_runner


    def __init__(self, args, callback=None, extra_vars={}, **parser_kwargs):
        """
        This custom method drops the dependency for 'options'
        Instead, PythonPlaybookOptions will read the kwargs and
        select sensible defaults when possible.

        This allows us to take advantage for pure-python calls.
        """
        self.args = args
        self.action = None
        self.callback = callback

        #This hack is required to properly map dict --> options.extra_vars
        if type(extra_vars) == dict:
            extra_vars = PythonPlaybookRunner._convert_extra_vars_to_option(extra_vars)
        parser_kwargs['extra_vars'] = extra_vars

        self.parser = PythonPlaybookOptions(
            args=args,
            **parser_kwargs
        )
        self.options = self.parser



