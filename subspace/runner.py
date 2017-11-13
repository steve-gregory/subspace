import logging
import os
import operator
import stat
import json

from ansible import constants as C
from ansible.cli import InvalidOptsParser
from ansible.cli.playbook import CLI, PlaybookCLI
from ansible.errors import AnsibleError, AnsibleOptionsError
from ansible.playbook.block import Block
from ansible.playbook.play_context import PlayContext

from subspace.exceptions import NoValidHosts
from subspace.internal.executor import SubspacePlaybookExecutor
from subspace.stats import SubspaceAggregateStats

try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()

default_logger = logging.getLogger(__name__)


class SubspaceOptParser(InvalidOptsParser):
    """
    (Formerly RunnerOptions)
    SubspaceOptParser class is used to replace Ansible OptParser
    """
    version = "subspace-1.0"
    usage="This strategy is not meant to be used from the CLI.",
    desc="Instead, call subspace from the Python REPL by using the command(s): ...",

    def __str__(self):
        options = self.__dict__
        return str(options)

    def __repr__(self):
        return self.__str__()

    def get_version(self):
        return self.version

    def __init__(
        self, verbosity=0, inventory=None, config_file=None, listhosts=None, subset=None, module_paths=None, extra_vars=[],
        forks=None, ask_vault_pass=False, vault_password_files=[], new_vault_password_files=[], vault_ids=[],
        output_file=None, tags=[], skip_tags=[], one_line=None, tree=None, ask_sudo_pass=False, ask_su_pass=False,
        sudo=False, sudo_user=None, become=False, become_method=None, su_user=None, become_ask_pass=False,
        ask_pass=False, private_key_file=None, remote_user='root', connection=None, timeout=None, ssh_common_args='',
        sftp_extra_args=None, scp_extra_args=None, ssh_extra_args='', poll_interval=None, seconds=None, check=False,
        syntax=None, diff=False, force_handlers=False, flush_cache=False, listtasks=None, listtags=None, module_path=None,
        # Subspace arguments below this line
        logger=None, args=None, **additional_kwargs):
        # Dynamic sensible defaults
        if not inventory:
            inventory = C.DEFAULT_HOST_LIST
        if not logger:
            logger = default_logger
        if not connection:
            connection = 'smart'
        if not become_method:
            become_method = 'sudo'
        if not su_user:
            su_user = C.DEFAULT_BECOME_USER
        if not become:
            become = C.DEFAULT_SU
        if not subset:
            subset = C.DEFAULT_SUBSET
        if not forks:
            forks = C.DEFAULT_FORKS
        default_vault_ids = C.DEFAULT_VAULT_IDENTITY_LIST
        if vault_ids:
            vault_ids = default_vault_ids + vault_ids
        else:
            vault_ids = default_vault_ids
        # Flags
        self.listhosts = listhosts
        self.listtasks = listtasks
        self.listtags = listtags
        self.syntax = syntax
        # Set your options
        self.verbosity = verbosity
        self.config_file = config_file
        self.inventory = inventory
        self.subset = subset
        self.module_paths = module_paths
        self.extra_vars = extra_vars
        self.forks = forks
        self.vault_ids = vault_ids
        self.ask_vault_pass = ask_vault_pass
        self.vault_password_files = vault_password_files
        self.new_vault_password_files = new_vault_password_files
        self.output_file = output_file
        self.tags = tags
        self.skip_tags = skip_tags
        self.one_line = one_line
        self.tree = tree
        self.ask_sudo_pass = ask_sudo_pass
        self.ask_su_pass = ask_su_pass
        self.sudo = sudo
        self.sudo_user = sudo_user
        self.su = become  # Remove when  ansible==2.5
        self.become = become
        self.become_method = become_method
        self.become_user = su_user
        self.su_user = su_user
        self.become_ask_pass = become_ask_pass
        self.ask_pass = ask_pass
        self.private_key_file = private_key_file
        self.remote_user = remote_user
        self.connection = connection
        self.timeout = timeout
        self.ssh_common_args = ssh_common_args
        self.sftp_extra_args = sftp_extra_args
        self.scp_extra_args = scp_extra_args
        self.ssh_extra_args = ssh_extra_args
        self.poll_interval = poll_interval
        self.seconds = seconds
        self.check = check
        self.diff = diff
        self.force_handlers = force_handlers
        self.flush_cache = flush_cache
        self.module_path = module_path
        # SUBSPACE-specific options below this line
        self.logger = logger
        if args and self.logger:
            self.logger.warn(
                "The following args passed to runner were ignored: %s" % args)
        if additional_kwargs and self.logger:
            self.logger.warn(
                "The following kwargs passed to options were ignored: %s" % additional_kwargs)
            self.ignored_kwargs = additional_kwargs



class PlaybookShell(PlaybookCLI):
    """
    Run ansible playbooks via python calls
    """

    inventory = None
    loader = None
    options = None
    extra_vars = None
    variable_manager = None

    def __init__(self, args, callback=None, **parser_kwargs):
        """
        This custom method drops the dependency for 'options'
        Instead, SubspaceOptParser will read the kwargs and
        select sensible defaults when possible.

        This allows us to take advantage for pure-python calls.
        """
        self.args = args
        self.parser = None
        self.action = None
        self.callback = callback
        self.parser = SubspaceOptParser(
            args=args,
            **parser_kwargs
        )
        self.options = self.parser
        # has already been computed by SubspaceOptParser, use it.
        # REVIEWERS NOTE: 'use_password' was a parser_kwarg that used to do the following:
        # # Become Pass Needed if not logging in as user root
        # if use_password:
        #     passwords = {'become_pass': use_password}
        #     else:
        #         passwords = None
        # I will remove this prior to merge, if confirmed that this is not being used anywhere
        # (I could not find it used anywhere) - SG
        # END REVIEWERS NOTE

    def run(self):
        """
        Runs the ansible command
        """
        super(PlaybookCLI, self).run()

        # Note: slightly wrong, this is written so that implicit localhost
        # Manage passwords
        sshpass = None
        becomepass = None
        passwords = {}

        # initial error check, to make sure all specified playbooks are accessible
        # before we start running anything through the playbook executor
        for playbook in self.args:
            if not os.path.exists(playbook):
                raise AnsibleError("the playbook: %s could not be found" % playbook)
            if not (os.path.isfile(playbook) or stat.S_ISFIFO(os.stat(playbook).st_mode)):
                raise AnsibleError("the playbook: %s does not appear to be a file" % playbook)

        # don't deal with privilege escalation or passwords when we don't need to
        if not self.options.listhosts and not self.options.listtasks and not self.options.listtags and not self.options.syntax:
            self.normalize_become_options()
            (sshpass, becomepass) = self.ask_passwords()
            passwords = {'conn_pass': sshpass, 'become_pass': becomepass}

        self.loader, self.inventory, self.variable_manager = self._play_prereqs(self.options)
        loader, inventory, variable_manager = self.loader, self.inventory, self.variable_manager

        # (which is not returned in list_hosts()) is taken into account for
        # warning if inventory is empty.  But it can't be taken into account for
        # checking if limit doesn't match any hosts.  Instead we don't worry about
        # limit if only implicit localhost was in inventory to start with.
        #
        # Fix this when we rewrite inventory by making localhost a real host (and thus show up in list_hosts())
        no_hosts = False
        if len(inventory.list_hosts()) == 0:
            # Empty inventory
            display.warning("provided hosts list is empty, only localhost is available")
            no_hosts = True
        inventory.subset(self.options.subset)
        if len(inventory.list_hosts()) == 0 and no_hosts is False:
            # Invalid limit
            raise AnsibleError("Specified --limit (%s) does not match any hosts" % self.options.subset)

        # flush fact cache if requested
        if self.options.flush_cache:
            self._flush_cache(inventory, variable_manager)

        # create the playbook executor, which manages running the plays via a task queue manager
        # pbex = PlaybookExecutor(playbooks=self.args, inventory=inventory, variable_manager=variable_manager, loader=loader, options=self.options,
        #                         passwords=passwords)
        # BEGIN CUSTOM Subspace injection
        pbex = SubspacePlaybookExecutor(
            playbooks=self.args,
            inventory=inventory,
            variable_manager=variable_manager,
            loader=loader,
            options=self.options,
            passwords=passwords)
        play_to_path_map = self._map_plays_to_playbook_path()
        pbex._tqm._stats = SubspaceAggregateStats(play_to_path_map)
        pbex._tqm.load_callbacks()
        pbex._tqm.send_callback(
            'start_logging',
            logger=self.options.logger,
            username=variable_manager.extra_vars.get('ATMOUSERNAME', "No-User"),
        )
        # END CUSTOM Subspace injection

        results = pbex.run()
        # BEGIN CUSTOM Subspace injection
        stats = pbex._tqm._stats
        self.stats = stats
        # Nonpersistent fact cache stores 'register' variables. We would like
        # to get access to stdout/stderr for specific commands and relay
        # some of that information back to the end user.
        self.results = dict(pbex._variable_manager._nonpersistent_fact_cache)
        # End CUSTOM Subspace injection

        if isinstance(results, list):
            for p in results:

                display.display('\nplaybook: %s' % p['playbook'])
                for idx, play in enumerate(p['plays']):
                    if play._included_path is not None:
                        loader.set_basedir(play._included_path)
                    else:
                        pb_dir = os.path.realpath(os.path.dirname(p['playbook']))
                        loader.set_basedir(pb_dir)

                    msg = "\n  play #%d (%s): %s" % (idx + 1, ','.join(play.hosts), play.name)
                    mytags = set(play.tags)
                    msg += '\tTAGS: [%s]' % (','.join(mytags))

                    if self.options.listhosts:
                        playhosts = set(inventory.get_hosts(play.hosts))
                        msg += "\n    pattern: %s\n    hosts (%d):" % (play.hosts, len(playhosts))
                        for host in playhosts:
                            msg += "\n      %s" % host

                    display.display(msg)

                    all_tags = set()
                    if self.options.listtags or self.options.listtasks:
                        taskmsg = ''
                        if self.options.listtasks:
                            taskmsg = '    tasks:\n'

                        def _process_block(b):
                            taskmsg = ''
                            for task in b.block:
                                if isinstance(task, Block):
                                    taskmsg += _process_block(task)
                                else:
                                    if task.action == 'meta':
                                        continue

                                    all_tags.update(task.tags)
                                    if self.options.listtasks:
                                        cur_tags = list(mytags.union(set(task.tags)))
                                        cur_tags.sort()
                                        if task.name:
                                            taskmsg += "      %s" % task.get_name()
                                        else:
                                            taskmsg += "      %s" % task.action
                                        taskmsg += "\tTAGS: [%s]\n" % ', '.join(cur_tags)

                            return taskmsg

                        all_vars = variable_manager.get_vars(play=play)
                        play_context = PlayContext(play=play, options=self.options)
                        for block in play.compile():
                            block = block.filter_tagged_tasks(play_context, all_vars)
                            if not block.has_tasks():
                                continue
                            taskmsg += _process_block(block)

                        if self.options.listtags:
                            cur_tags = list(mytags.union(all_tags))
                            cur_tags.sort()
                            taskmsg += "      TASK TAGS: [%s]\n" % ', '.join(cur_tags)

                        display.display(taskmsg)

            return 0
        else:
            return results

    # CUSTOM subspace functions below this line
    def _get_playbook_name(self, playbook):
        key_name = ''
        with open(playbook,'r') as the_file:
            for line in the_file.readlines():
                if 'name:' in line.strip():
                    # This is the name you will find in stats.
                    key_name = line.replace('name:','').replace('- ','').strip()
        if not key_name:
            raise Exception(
                "Unnamed playbooks will not allow CustomSubspaceStats to work properly.")
        return key_name

    def _get_playbook_map(self):
        """
        """
        playbook_map = {
            self._get_playbook_name(playbook): playbook
            for playbook in self.args}
        if len(playbook_map) != len(self.args):
            raise ValueError("Non unique names in your playbooks will not allow CustomSubspaceStats to work properly. %s" % self.args)
        return playbook_map


# For compatability
class Runner(PlaybookShell):

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
    def _get_playbook_args(cls, playbook_path, limit_playbooks):
        """
        Walk the directory, return an ordered list of playbook objects.
        If 'limit_playbooks' is included, only return matching playbooks within the directory.
        """
        if not isinstance(playbook_path, basestring):
            raise TypeError(
                "Expected 'playbook_path' as string,"
                " received %s" % type(playbook_path))
        # Convert file path to list of playbooks:
        if not os.path.exists(playbook_path):
            raise ValueError("Could not find path: %s" % (playbook_path,))

        if os.path.isdir(playbook_path):
            playbook_list = cls._get_playbook_files(
                playbook_path, limit_playbooks)
        else:
            playbook_list = [playbook_path]
        return playbook_list

    @classmethod
    def _get_playbook_files(cls, playbook_dir, limit=[]):
        """
        Walk the Directory structure and return an ordered list of
        playbook objects.

        :directory: The directory to walk and search for playbooks.
        Notes: * Playbook files are identified as ending in .yml
        """
        return [pb for pb in cls._get_files(playbook_dir)
                if not limit or pb.split('/')[-1] in limit]

    @classmethod
    def _get_files(cls, directory):
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
            if os.path.isdir(a_dir) and "playbooks" in a_dir:
                for f in files_in_dir:
                    if os.path.splitext(f)[1] == ".yml":
                        files.append(os.path.join(a_dir, f))
        return files

    @classmethod
    def factory(
            cls, playbook_path,
            limit_hosts=None, limit_playbooks=None,
            extra_vars={},
            **runner_options):
        """
        Walk the Directory structure and return an ordered list of
        playbook objects.

        :playbook_path: The directory/path to use for playbook list.
        :runner_options: These keyword args will be passed into Runner

        Notes: * Playbook files are identified as ending in .yml
               * Playbooks are not executed until calling Runner.run()
        """

        extra_vars_option = cls._convert_extra_vars_to_option(extra_vars)

        args = cls._get_playbook_args(playbook_path, limit_playbooks)
        runner = Runner(
            args,
            playbook_dir=playbook_path,
            extra_vars=extra_vars_option,
            **runner_options
        )
        return runner
