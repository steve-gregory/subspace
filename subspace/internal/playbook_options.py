import logging
from ansible import constants as C
from ansible.cli import InvalidOptsParser

try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()

default_logger = logging.getLogger(__name__)


class PythonPlaybookOptions(InvalidOptsParser):
    """
    PythonPlaybookOptions class is used to replace
    human-interaction and argparse, when creating a
    PythonPlaybookRunner()

    As settings are added/removed in ansible, you may need
    to change the acceptable kwargs passed to PythonPlaybookOptions
    """
    version = "subspace-1.0"
    usage = "This strategy is not meant to be used from the CLI.",
    desc = "Instead, call subspace from the Python REPL by using the command(s): ...",

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
        sudo=False, sudo_user=None, become=False, become_method='sudo', su_user=None, become_ask_pass=False, become_pass=None,
        ask_pass=False, private_key_file=None, remote_user='root', connection='smart', conn_pass=None, timeout=None, ssh_common_args='',
        sftp_extra_args=None, scp_extra_args=None, ssh_extra_args='', poll_interval=None, seconds=None, check=False,
        syntax=None, diff=False, force_handlers=False, flush_cache=False, listtasks=None, listtags=None, module_path=None,
        # Subspace arguments below this line
        logger=None, additional_args=None, **additional_kwargs):

        # Use ansible constants to set sensible defaults
        # This will search the PATH and set values accordingly
        # based on first ansible.cfg file found in PATH
        if not inventory:
            inventory = C.DEFAULT_HOST_LIST
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
        # Flags (These will never be set, but options requires these variables to function as a standalone replacement.)
        self.listhosts = listhosts
        self.listtasks = listtasks
        self.listtags = listtags
        self.syntax = syntax
        # Options that will be set often
        self.verbosity = verbosity
        self.config_file = config_file
        self.inventory = inventory
        self.subset = subset
        self.module_paths = module_paths
        self.extra_vars = extra_vars
        # Options that will be set less often
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
        self.become_pass = become_pass
        self.become_ask_pass = become_ask_pass
        self.ask_pass = ask_pass
        self.private_key_file = private_key_file
        self.remote_user = remote_user
        self.connection = connection
        self.conn_pass = conn_pass
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
        if not logger:
            logger = default_logger

        self.logger = logger
        if additional_args:
            self.logger.warn(
                "The following additional_args passed to runner were ignored: %s" % additional_args)
            self.ignored_args = additional_args
        if additional_kwargs:
            self.logger.warn(
                "The following kwargs passed to options were ignored: %s" % additional_kwargs)
            self.ignored_kwargs = additional_kwargs
