import logging
from collections import namedtuple

state = namedtuple('Result', 'result, value, vars')

log = logging.getLogger(__name__)


class CoreBase(dict):
    def __init__(self, options):
        options['system.userid'] = '99'  # plugin-dev
        self.options = options
        self.plugin_path = '/'.join(self.__class__.__module__.split('.')[:-1])

        dict.__init__(self, self.options)

    def __getitem__(self, key):
        # if key not in self.options and key != 'system.userid':
        #     log.warning('{} not in self.options={}'.format(key, self.options))
        return dict.__getitem__(self, key)

    def __getattr__(self, key):
        try:
            return self.options[key]
        except KeyError as err:
            raise AttributeError(err)


class IAgentCore(CoreBase):
    def action(self, action_code, **kwargs):
        try:
            log.debug('🍋 Action start for %r' % action_code)
            res = getattr(self, action_code)(**kwargs)
            return self.widget_suc(vars=res)
        except FileNotFoundError as err:
            return self.widget_err(err_type='FileNotFound', message=err)
        except ConnectionError as err:
            return self.widget_err(err_type='ConnectionError', message=err)
        except Exception as err:
            return self.widget_err(err_type='Exception', message=err)

    @staticmethod
    def widget_suc(**kwargs):
        return dict(result='success', value='success', vars=kwargs.get('vars', {}))

    def widget_err(self, err_type, message):
        log.exception(f"{type}: {message}")
        return dict(result='error', value=err_type, vars=str(message))
