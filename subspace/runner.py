import os
import stat

from ansible.errors import AnsibleError
from ansible.cli.playbook import PlaybookCLI
from ansible.playbook.block import Block
from ansible.playbook.play_context import PlayContext

from subspace.internal.executor import SubspacePlaybookExecutor
from subspace.internal.playbook import PythonPlaybookRunner
from subspace.stats import SubspaceAggregateStats

try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()


class SubspacePlaybookRunner(PythonPlaybookRunner):
    """
    A SubspacePlaybookRunner is responsible for:
    - Replace creation of playbook executor, with custom SubspacePlaybookExecutor, which manages running the plays via a custom task queue manager: SubspaceTaskQueueManager
    - Ensuring that the default 'strategy' for all playbooks is Subspace (Handled by SubspaceTaskQueueManager)
    - In addition to the properties included in PythonPlaybookRunner, a SubspacePlaybookRunner will include the following variables after a run():
      - loader
      - inventory
      - variable_manager
      - logger
      - stats
      - *results (AKA variable_manager._nonpersistent_fact_cache)
    * = Nonpersistent fact cache stores 'register' variables, this will include the stdout/stderr/exit code for specific commands.
    """
    results = None

    def _store_playbook_results(self, playbook_executor):
        # What we are storing
        ansible_playbook_summary_stats = playbook_executor._tqm._stats
        ansible_playbook_register_vars = playbook_executor._variable_manager._nonpersistent_fact_cache

        self.stats = ansible_playbook_summary_stats
        if not self.results:
            self.results = dict(ansible_playbook_register_vars)
        else:
            self.results.update(dict(ansible_playbook_register_vars))
        return

    # def _get_playbook_map(self):
    #         """
    #         """
    #         play_to_path_map = {}
    #         for playbook_path in self.args:
    #             keys = self._get_playbook_name(playbook_path)
    #             for key in keys:
    #                 play_to_path_map[key] = playbook_path
    #         return play_to_path_map

    def _get_playbook_name(self, playbook):
        key_name = ''
        with open(playbook, 'r') as the_file:
            for line in the_file.readlines():
                if 'name:' in line.strip():
                    # This is the name you will find in stats.
                    key_name = line.replace('name:', '').replace('- ', '').strip()
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
            raise ValueError(
                "Non-unique names in your playbooks will not allow "
                "CustomSubspaceStats to work properly. %s" % self.args)
        return playbook_map

    def run(self):
        """
        Runs the ansible command.

        NOTE: This is a _direct copy_ of ansible/cli/playbook.PlaybookCLI
              run() method
              Changes made by subspace are clearly wrapped with the following lines:
              ```
              # BEGIN CUSTOM Subspace injection
              ... code that was changed...
              # END CUSTOM Subspace injection
              ```
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

        loader, inventory, variable_manager = self._play_prereqs(self.options)
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

        # BEGIN CUSTOM Subspace injection -- Pre-playbook execution hook
        # - Use the SubspacePlaybookExecutor instead of PlaybookExecutor
        # - Store loader, inventory, and variable_manager for inspection after execution.
        # - Initialize custom logger

        #### create the playbook executor, which manages running the plays via a task queue manager
        ### pbex = PlaybookExecutor(playbooks=self.args, inventory=inventory, variable_manager=variable_manager, loader=loader, options=self.options,

        (self.loader, self.inventory, self.variable_manager) = (
                loader, inventory, variable_manager)

        pbex = SubspacePlaybookExecutor(
            playbooks=self.args,
            inventory=inventory,
            variable_manager=variable_manager,
            loader=loader,
            options=self.options,
            passwords=passwords)
        play_to_path_map = self._get_playbook_map()

        pbex._tqm._stats = SubspaceAggregateStats(play_to_path_map)

        pbex._tqm.load_callbacks()
        pbex._tqm.send_callback(
            'start_logging',
            logger=self.options.logger,
            username=variable_manager.extra_vars.get(
                'ATMOUSERNAME', "No-User"),
        )
        # END CUSTOM Subspace injection -- We rely on our ansible-magic to take it from here..

        results = pbex.run()

        # BEGIN CUSTOM Subspace injection -- Post-playbook execution hook
        self._store_playbook_results(pbex)
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
