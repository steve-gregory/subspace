from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

import multiprocessing
import os
import tempfile

from ansible.errors import AnsibleError
from ansible.executor.play_iterator import PlayIterator
from ansible.executor.stats import AggregateStats
from ansible.playbook.play_context import PlayContext
from ansible.plugins.loader import strategy_loader
from ansible.template import Templar
from ansible.utils.helpers import pct_to_int
from ansible.vars.hostvars import HostVars
from ansible.vars.reserved import warn_if_reserved

# Subspace-specific includes
from ansible.executor.task_queue_manager import TaskQueueManager
# End-subspace-specific includes

try:
    from __main__ import display
except ImportError:
    from ansible.utils.display import Display
    display = Display()


__all__ = ['SubspaceTaskQueueManager']


class SubspaceTaskQueueManager(TaskQueueManager):

    '''
    The SubspaceTaskQueueManager works identically to the TaskQueueManager
    *WITH ONE EXCEPTION:*
      - the default strategy for all playbooks is 'subspace'
    '''
    default_strategy = 'subspace'  # Use the subspace strategy by default.

    def run(self, play):
        '''
        Iterates over the roles/tasks in a play, using the given (or default)
        strategy for queueing tasks. The default is the linear strategy, which
        operates like classic Ansible by keeping all hosts in lock-step with
        a given task (meaning no hosts move on to the next task until all hosts
        are done with the current task).
        '''

        if not self._callbacks_loaded:
            self.load_callbacks()

        all_vars = self._variable_manager.get_vars(play=play)
        warn_if_reserved(all_vars)
        templar = Templar(loader=self._loader, variables=all_vars)

        new_play = play.copy()
        new_play.post_validate(templar)
        new_play.handlers = new_play.compile_roles_handlers() + new_play.handlers

        self.hostvars = HostVars(
            inventory=self._inventory,
            variable_manager=self._variable_manager,
            loader=self._loader,
        )

        play_context = PlayContext(new_play, self._options, self.passwords, self._connection_lockfile.fileno())
        for callback_plugin in self._callback_plugins:
            if hasattr(callback_plugin, 'set_play_context'):
                callback_plugin.set_play_context(play_context)

        self.send_callback('v2_playbook_on_play_start', new_play)

        # initialize the shared dictionary containing the notified handlers
        self._initialize_notified_handlers(new_play)


        # build the iterator
        iterator = PlayIterator(
            inventory=self._inventory,
            play=new_play,
            play_context=play_context,
            variable_manager=self._variable_manager,
            all_vars=all_vars,
            start_at_done=self._start_at_done,
        )

        # adjust to # of workers to configured forks or size of batch, whatever is lower
        self._initialize_processes(min(self._options.forks, iterator.batch_size))

        ##################
        # Subspace snippet

        # # load the specified strategy (or the default linear one)
        # strategy = strategy_loader.get(new_play.strategy, self)

        # IF the method for loading strategy fails,
        #  this hack will ensure
        # 'Subspace' linear strategy is what gets used.
        strategy = self._ensure_subspace_plugin(new_play)

        if strategy is None:
            raise AnsibleError("Invalid play strategy specified: %s" % new_play.strategy, obj=play._ds)
        # END subspace snippet
        ##################

        # Because the TQM may survive multiple play runs, we start by marking
        # any hosts as failed in the iterator here which may have been marked
        # as failed in previous runs. Then we clear the internal list of failed
        # hosts so we know what failed this round.
        for host_name in self._failed_hosts.keys():
            host = self._inventory.get_host(host_name)
            iterator.mark_host_failed(host)

        self.clear_failed_hosts()

        # during initialization, the PlayContext will clear the start_at_task
        # field to signal that a matching task was found, so check that here
        # and remember it so we don't try to skip tasks on future plays
        if getattr(self._options, 'start_at_task', None) is not None and play_context.start_at_task is None:
            self._start_at_done = True

        # and run the play using the strategy and cleanup on way out
        play_return = strategy.run(iterator, play_context)

        # now re-save the hosts that failed from the iterator to our internal list
        for host_name in iterator.get_failed_hosts():
            self._failed_hosts[host_name] = True

        strategy.cleanup()
        self._cleanup_processes()
        return play_return

    def _ensure_subspace_plugin(self, new_play):
        """
        # This method will force-override plays to use 'subspace' as the default strategy.
        # In the future, there may be a better/cleaner way to do this
        # When such a time comes, feel free to drop this function.
        """
        from subspace.plugins.strategy.subspace import StrategyModule

        new_play.strategy = self.default_strategy

        # FIXME test if this can be removed in 2.4: Removing this line still causes failures in ansible2.3
        subspace_dir = os.path.dirname(__file__)
        strategy_loader.config = os.path.join(subspace_dir, 'plugins/strategy')

        # Load subspace strategy
        strategy = strategy_loader.get(new_play.strategy, self)
        if strategy is None or not isinstance(strategy, StrategyModule):
            strategy = StrategyModule(self)
        if not isinstance(strategy, StrategyModule):
            raise AnsibleError("Invalid play strategy specified: %s" % new_play.strategy, obj=new_play._ds)
        return strategy

